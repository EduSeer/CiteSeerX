# Script for ingesting Pedocs documents into citeseerx
use File::Copy;
use DBI;
use XML::LibXML;
use Time::localtime;

my $inputFolder = $ARGV[0];
my $outputFolder = $ARGV[1];

if (!defined $inputFolder || !defined $outputFolder) {
    print "Usage: $0 $1 inputFolder outputFolder\ninputFolder: folder containing Pedocs PDF files\noutputFolder: the folder which will contain the preprocessed files";
    exit;
}

# MySQL access 
my $dsn = "dbi:mysql:pedocs:localhost:3306"; 
my $user = "csx-devel"; 
my $pass = "csx-devel";

# Perl scripts for PDF-to-TXT, ParsCit, BuildXML
my $extractTextPath = "/srv/seersuite/trunk/dist/services/FileConversionService/bin/extractText.pl";
my $parsCitPath = "/srv/seersuite/trunk/dist/services/ParsCit/bin/citeExtract.pl";
my $buildXMLPath = "/srv/seersuite/trunk/src/perl/BatchExtractor/bin/buildXML.pl";


my $dbh = DBI->connect($dsn, $user, $pass) or die "Can't connect to the DB: $DBI::errstr\n";

opendir(DIR, $inputFolder);
@FILES = sort grep{!/^\./} readdir(DIR); 

my $min = 1000000;
my $max = 0;

# Use folder name as the source_opus id in database
foreach my $sourceOpusID (@FILES){
  
  # PDF is in pdf subfolder
  opendir (DOCDIR, "$inputFolder/$sourceOpusID/pdf/");
  @dirTmp = grep { /.pdf$/ } readdir(DOCDIR);
  my $inputFile = $dirTmp[0];
  $inputFile = "$inputFolder/$sourceOpusID/pdf/$inputFile";
  
  # Save all files with source_opus id as the file name
  $outputFile = "$outputFolder/$sourceOpusID";

  print "Process: $sourceOpusID \n"; 
  copy($inputFile, "$outputFile.pdf") or die "Couldn't copy: $!";

  print "Converting to TXT...\n";
  system("perl -CSD $extractTextPath $outputFile.pdf");
  
  print "Extracting citations...\n";
  system("perl -CSD $parsCitPath $outputFile.txt $outputFile.parscit"); 
  
  print "Extracting header...\n";
  createHeaderFromDB($dbh, $sourceOpusID, "$outputFile.header");
  createMetFile("$outputFile.met");

  # Save min and max source_opus ids in order 
  # to use them with buildXML script later
  if ($sourceOpusID < $min){
    $min = $sourceOpusID;
  } 

  if ($sourceOpusID > $max){
    $max = $sourceOpusID;
  } 
}

print "Building XML...\n";
system("perl -CSD $buildXMLPath $outputFolder $min $max");


print "\nFINISHED\n";

#####
# Creates .header file by using data from pedocs db
#####
sub createHeaderFromDB {
  my ($dbh, $sourceOpusID, $outputFilePath) = @_;
  my $query = "SELECT title, description, subject_uncontrolled_german, date_year, date_published "
             ."FROM opus WHERE source_opus = $sourceOpusID";
 
  my $sth = $dbh->prepare($query) or die "Can't prepare SQL statement: ", $dbh->errstr(), "\n";
  $sth->execute or die "Can't execute SQL statement: ", $sth->errstr(), "\n";
 
  my $doc = XML::LibXML::Document->new('1.0','UTF-8');
  # We need to set algorithm like if the header was created with SVM HeaderParse.
  # This is required by CSX ingestion script
  my $root = $doc->createElement("algorithm");
  $root->setAttribute("name", "SVM HeaderParse"); 
  $root->setAttribute("version", "0.2");
  $doc->setDocumentElement($root);

  while ($row = $sth->fetchrow_hashref() ) {
     my $title = $$row{'title'};
     my $abstract = $$row{'description'};
     # Extract keywords
     my @keywords = split /; /, $$row{'subject_uncontrolled_german'};
     # Use year of publication as its date
     my $date = $$row{'date_year'};     
     $root->appendTextChild("title", $title);
     $root->appendTextChild("abstract", $abstract);
     $root->appendTextChild("date", $date);

     if (@keywords > 0) {       
       my $keywordsNode = XML::LibXML::Element->new("keywords");
       $root->appendChild($keywordsNode);
       # Save each keyword in its own XML element
       foreach my $keyword (@keywords){
         $keywordsNode->appendTextChild("keyword", $keyword);
       }
     }
  }
  
  # Extract authors
  $query = "SELECT creator_name FROM opus_autor " 
           ."WHERE source_opus=$sourceOpusID ORDER BY reihenfolge";  
  $sth = $dbh->prepare($query);
  $sth->execute or die "Can't execute SQL statement: ", $sth->errstr(), "\n";
  
  $authorsNode = XML::LibXML::Element->new("authors");
  $root->appendChild($authorsNode);
  
  while($row = $sth->fetchrow_hashref()){
    # Convert author names from 'Last_name, First_name' to 'First_name Last_name'
    @nameParts = split /, /, $$row{'creator_name'};
    my $name;
    if(@nameParts > 1){
      $name = $nameParts[1]." ".$nameParts[0];
    }else{
      $name = $nameParts[0];
    }
    $authorNode = XML::LibXML::Element->new("author");
    $authorsNode->appendChild($authorNode);
    $authorNode->appendTextChild("name", $name);
  }
  
  $root->appendTextChild("validHeader", "1");
  open (FILE, ">$outputFilePath");
  # Save to file without '<?xml version="1.0" encoding="UTF-8"?>'
  # as it causes problems while ingestion
  print FILE substr($doc->toString(1),39);
  close(FILE);
  
}

#####
# Creates .met file with empty root element
####
sub createMetFile{
 my ($outputFilePath) = @_;
 my $doc = XML::LibXML::Document->new('1.0','UTF-8');
 my $root = $doc->createElement("CrawlData");
 $doc->setDocumentElement($root);
 
 open (FILE, ">$outputFilePath"); 
 print FILE $doc->toString(1);
 close(FILE);

 #Possible structure of .met file:
 #<?xml version="1.0" encoding="UTF-8"?>
 # <CrawlData>
 #  <crawlDate/>
 #  <lastModified/>
 #  <url/>
 #  <parentUrl/>
 #  <SHA1/>
 # </CrawlData>


}
