/*
 * Copyright 2007 Penn State University
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.psu.citeseerx.dao2.logic;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Date;
import java.util.List;

import org.springframework.dao.DataAccessException;

import com.lowagie.text.pdf.PdfReader;

import edu.psu.citeseerx.dao2.AckDAO;
import edu.psu.citeseerx.dao2.AdminDAO;
import edu.psu.citeseerx.dao2.AlgorithmDAO;
import edu.psu.citeseerx.dao2.AuthorDAO;
import edu.psu.citeseerx.dao2.CitationDAO;
import edu.psu.citeseerx.dao2.CiteChartDAO;
import edu.psu.citeseerx.dao2.DocumentDAO;
import edu.psu.citeseerx.dao2.ExternalLinkDAO;
import edu.psu.citeseerx.dao2.FileDAO;
import edu.psu.citeseerx.dao2.FileSysDAO;
import edu.psu.citeseerx.dao2.GeneralStatistics;
import edu.psu.citeseerx.dao2.HubDAO;
import edu.psu.citeseerx.dao2.KeywordDAO;
import edu.psu.citeseerx.dao2.LegacyIDDAO;
import edu.psu.citeseerx.dao2.RedirectPDFDAO;
import edu.psu.citeseerx.dao2.TableDAO;
import edu.psu.citeseerx.dao2.TagDAO;
import edu.psu.citeseerx.dao2.UniqueAuthorDAO;
import edu.psu.citeseerx.dao2.UniqueAuthorVersionDAO;
import edu.psu.citeseerx.dao2.UnknownRepositoryException;
import edu.psu.citeseerx.dao2.VersionDAO;
import edu.psu.citeseerx.domain.Acknowledgment;
import edu.psu.citeseerx.domain.Algorithm;
import edu.psu.citeseerx.domain.Author;
import edu.psu.citeseerx.domain.CheckSum;
import edu.psu.citeseerx.domain.Citation;
import edu.psu.citeseerx.domain.DOIInfo;
import edu.psu.citeseerx.domain.Document;
import edu.psu.citeseerx.domain.DocumentFileInfo;
import edu.psu.citeseerx.domain.ExternalLink;
import edu.psu.citeseerx.domain.Hub;
import edu.psu.citeseerx.domain.Keyword;
import edu.psu.citeseerx.domain.LinkType;
import edu.psu.citeseerx.domain.PDFRedirect;
import edu.psu.citeseerx.domain.Table;
import edu.psu.citeseerx.domain.Tag;
import edu.psu.citeseerx.domain.ThinDoc;
import edu.psu.citeseerx.domain.UniqueAuthor;
import edu.psu.citeseerx.utility.FileNamingUtils;

/**
 * Provides a single point access to all Document related persistent storage operations
 *
 * @author Isaac Councill
 * @version $Rev$ $Date$
 */
public class CSXDAOImpl
    implements CSXDAO
{

    private AckDAO ackDAO;
    private AdminDAO adminDAO;
    private AuthorDAO authDAO;
    private CitationDAO citeDAO;
    private CiteChartDAO citeChartDAO;
    private GeneralStatistics generalStatistics;
    private DocumentDAO docDAO;
    private FileDAO fileDAO;
    private FileSysDAO fileSysDAO;
    private HubDAO hubDAO;
    private KeywordDAO keywordDAO;
    private LegacyIDDAO legacyIDDAO;
    private TagDAO tagDAO;
    private UniqueAuthorDAO uauthDAO;
    private UniqueAuthorVersionDAO uauthVersionDAO;
    private VersionDAO versionDAO;
    private ExternalLinkDAO externalLinkDAO;
    private TableDAO tableDAO;
    private AlgorithmDAO algorithmDAO;
    private RedirectPDFDAO redirectPDFDAO;

    public void setAckDAO(final AckDAO ackDAO)
    {
        this.ackDAO = ackDAO;
    } // - setAckDAO

    public void setAdminDAO(final AdminDAO adminDAO)
    {
        this.adminDAO = adminDAO;
    } // - setAdminDAO

    public void setAuthDAO(final AuthorDAO authDAO)
    {
        this.authDAO = authDAO;
    } // - setAuthDAO

    public void setUniqueAuthDAO(final UniqueAuthorDAO uauthDAO)
    {
        this.uauthDAO = uauthDAO;
    }

    public void setUniqueAuthVersionDAO(final UniqueAuthorVersionDAO uauthVersionDAO)
    {
        this.uauthVersionDAO = uauthVersionDAO;
    }

    public void setCiteChartDAO(final CiteChartDAO citeChartDAO)
    {
        this.citeChartDAO = citeChartDAO;
    } // - setCiteChartDAO

    public void setCiteDAO(final CitationDAO citeDAO)
    {
        this.citeDAO = citeDAO;
    } // - setCiteDAO

    public void setDocDAO(final DocumentDAO docDAO)
    {
        this.docDAO = docDAO;
    } // - setDocDAO

    public void setFileDAO(final FileDAO fileDAO)
    {
        this.fileDAO = fileDAO;
    } // - setFileDAO

    public void setFileSysDAO(final FileSysDAO fileSysDAO)
    {
        this.fileSysDAO = fileSysDAO;
    } // - setFileSysDAO

    public void setHubDAO(final HubDAO hubDAO)
    {
        this.hubDAO = hubDAO;
    } // - setHubDAO

    public void setKeywordDAO(final KeywordDAO keywordDAO)
    {
        this.keywordDAO = keywordDAO;
    } // - setKeywordDAO

    public void setLegacyIDDAO(final LegacyIDDAO legacyIDDAO)
    {
        this.legacyIDDAO = legacyIDDAO;
    } // - setLegacyIDDAO

    public void setTagDAO(final TagDAO tagDAO)
    {
        this.tagDAO = tagDAO;
    } // - setTagDAO

    public void setVersionDAO(final VersionDAO versionDAO)
    {
        this.versionDAO = versionDAO;
    } // - setVersionDAO

    public void setExternalLinkDAO(final ExternalLinkDAO externalLinkDAO)
    {
        this.externalLinkDAO = externalLinkDAO;
    } // - setExternalLinkDAO

    public void setTableDAO(final TableDAO tableDAO)
    {
        this.tableDAO = tableDAO;
    } // - setTableDAO

    public void setGeneralStatistics(final GeneralStatistics generalStatistics)
    {
        this.generalStatistics = generalStatistics;
    }

    public void setAlgorithmDAO(final AlgorithmDAO algorithmDAO)
    {
        this.algorithmDAO = algorithmDAO;
    } // - setAlgorithmDAO

    public void setRedirectPDFDAO(final RedirectPDFDAO redirectPDFDAO)
    {
        this.redirectPDFDAO = redirectPDFDAO;
    }

    // /////////////////////////////////////////////////////
    // CSX Operations
    // /////////////////////////////////////////////////////

    /*
     * (non-Javadoc)
     *
     * @see
     * edu.psu.citeseerx.dao2.logic.CSXOperations#insertDocumentEntry(edu.psu.citeseerx.domain.Document
     * )
     */
    @Override
    public void insertDocumentEntry(final Document doc)
        throws DataAccessException
    {
        this.docDAO.insertDocument(doc);
    } // - insertDocumentEntry

    /*
     * (non-Javadoc)
     *
     * @see
     * edu.psu.citeseerx.dao2.logic.CSXOperations#importDocument(edu.psu.citeseerx.domain.Document)
     */
    @Override
    public void importDocument(final Document doc)
        throws DataAccessException, IOException
    {
        String doi = doc.getDatum(Document.DOI_KEY);

        this.docDAO.insertDocumentSrc(doc);

        DocumentFileInfo finfo = doc.getFileInfo();
        for (String url : finfo.getUrls()) {
            this.hubDAO.insertUrl(doi, url);
        }
        for (Hub hub : finfo.getHubs()) {
            for (String url : finfo.getUrls()) {
                this.hubDAO.addHubMapping(hub, url, doi);
            }
        }
        for (CheckSum sum : finfo.getCheckSums()) {
            sum.setDOI(doi);
            this.fileDAO.insertChecksum(sum);
        }

        this.insertAuthors(doi, doc.getAuthors());

        this.insertCitations(doi, doc.getCitations());

        this.insertAcknowledgments(doi, doc.getAcknowledgments());

        this.insertKeywords(doi, doc.getKeywords());

        for (Tag tag : doc.getTags()) {
            this.tagDAO.addTag(doi, tag.getTag());
        }

        this.fileSysDAO.writeXML(doc);

    } // - importDocument

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.logic.CSXOperations#getDocumentFromDB(java.lang.String, boolean,
     * boolean, boolean, boolean, boolean, boolean)
     */
    @Override
    public Document getDocumentFromDB(final String doi, final boolean getCitations,
            final boolean getContexts, final boolean getSource, final boolean getAcks,
            final boolean getKeywords, final boolean getTags)
        throws DataAccessException
    {

        Document doc = this.docDAO.getDocument(doi, getSource);

        if (doc == null)
            return null;

        DocumentFileInfo finfo = doc.getFileInfo();
        List<String> urls = this.hubDAO.getUrls(doi);
        for (Object o : urls) {
            finfo.addUrl((String) o);
        }

        List<Author> authors = this.authDAO.getDocAuthors(doi, getSource);
        for (Author author : authors) {
            doc.addAuthor(author);
        }

        if (getCitations) {
            List<Citation> citations = this.citeDAO.getCitations(doi, getContexts);
            for (Citation citation : citations) {
                doc.addCitation(citation);
            }
        }
        if (getAcks) {
            List<Acknowledgment> acks = this.ackDAO.getAcknowledgments(doi, getContexts, getSource);
            for (Acknowledgment ack : acks) {
                doc.addAcknowledgment(ack);
            }
        }
        if (getKeywords) {
            List<Keyword> keywords = this.keywordDAO.getKeywords(doi, getSource);
            for (Keyword keyword : keywords) {
                doc.addKeyword(keyword);
            }
        }
        if (getTags) {
            List<Tag> tags = this.tagDAO.getTags(doi);
            doc.setTags(tags);
        }

        return doc;

    } // - getDocumentFromDB

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.logic.CSXOperations#getDocumentFromDB(java.lang.String, boolean,
     * boolean)
     */
    @Override
    public Document getDocumentFromDB(final String doi, final boolean getContexts,
            final boolean getSource)
        throws DataAccessException
    {

        return this.getDocumentFromDB(doi, true, getContexts, getSource, true, true, true);

    } // - getDocumentFromDB

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.logic.CSXOperations#getDocumentFromDB(java.lang.String)
     */
    @Override
    public Document getDocumentFromDB(final String doi)
        throws DataAccessException
    {

        return this.getDocumentFromDB(doi, false, false, false, false, false, false);
    } // - getDocumentFromDB

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.logic.CSXOperations#getDocumentFromXML(java.lang.String)
     */
    @Override
    public Document getDocumentFromXML(final String doi)
        throws DataAccessException, IOException
    {
        Document doc = this.docDAO.getDocument(doi, false);
        String repID = doc.getFileInfo().getDatum(DocumentFileInfo.REP_ID_KEY);
        String relPath = FileNamingUtils.buildXMLPath(doi);
        return this.fileSysDAO.getDocFromXML(repID, relPath);

    } // - getDocumentFromXML

    /*
     * (non-Javadoc)
     *
     * @see
     * edu.psu.citeseerx.dao2.logic.CSXOperations#updateDocumentData(edu.psu.citeseerx.domain.Document
     * )
     */
    @Override
    public void updateDocumentData(final Document doc)
        throws DataAccessException, IOException
    {
        this.updateDocumentData(doc, true, true, true, true);
    } // - updateDocumentData

    /*
     * (non-Javadoc)
     *
     * @see
     * edu.psu.citeseerx.dao2.logic.CSXOperations#updateDocumentData(edu.psu.citeseerx.domain.Document
     * , boolean, boolean, boolean, boolean)
     */
    @Override
    public void updateDocumentData(final Document doc, final boolean updateAuthors,
            final boolean updateCitations, final boolean updateAcknowledgements,
            final boolean updateKeywords)
        throws DataAccessException, IOException
    {

        String doi = doc.getDatum(Document.DOI_KEY);

        this.docDAO.updateDocument(doc);
        // fileDAO.updateFileInfo(doi, doc.getFileInfo(), con);

        if (updateAuthors) {
            this.authDAO.deleteAuthors(doi);
            this.insertAuthors(doi, doc.getAuthors());
        }

        if (updateCitations) {
            this.citeDAO.deleteCitations(doi);
            this.insertCitations(doi, doc.getCitations());
        }

        if (updateAcknowledgements) {
            this.ackDAO.deleteAcknowledgments(doi);
            this.insertAcknowledgments(doi, doc.getAcknowledgments());
        }

        if (updateKeywords) {
            this.keywordDAO.deleteKeywords(doi);
            this.insertKeywords(doi, doc.getKeywords());
        }

    } // - updateDocumentData

    // /////////////////////////////////////////////////////
    // Acknowledgment DAO
    // /////////////////////////////////////////////////////

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.AckDAO#deleteAcknowledgment(java.lang.Long)
     */
    @Override
    public void deleteAcknowledgment(final Long ackID)
        throws DataAccessException
    {
        this.ackDAO.deleteAcknowledgment(ackID);
    } // - deleteAcknowledgment

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.AckDAO#deleteAcknowledgments(java.lang.String)
     */
    @Override
    public void deleteAcknowledgments(final String doi)
        throws DataAccessException
    {
        this.ackDAO.deleteAcknowledgments(doi);
    } // - deleteAcknowledgments

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.AckDAO#deleteAckContexts(java.lang.Long)
     */
    @Override
    public void deleteAckContexts(final Long ackID)
        throws DataAccessException
    {
        this.ackDAO.deleteAckContexts(ackID);
    } // - deleteAckContexts

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.AckDAO#getAcknowledgments(java.lang.String, boolean, boolean)
     */
    @Override
    public List<Acknowledgment> getAcknowledgments(final String doi, final boolean getContexts,
            final boolean getSource)
        throws DataAccessException
    {
        return this.ackDAO.getAcknowledgments(doi, getContexts, getSource);
    } // - getAcknowledgments

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.AckDAO#getAckContexts(java.lang.Long)
     */
    @Override
    public List<String> getAckContexts(final Long ackID)
        throws DataAccessException
    {
        return this.ackDAO.getAckContexts(ackID);
    } // - getAckContexts

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.AckDAO#insertAcknowledgment(java.lang.String,
     * edu.psu.citeseerx.domain.Acknowledgment)
     */
    @Override
    public void insertAcknowledgment(final String doi, final Acknowledgment ack)
        throws DataAccessException
    {
        this.ackDAO.insertAcknowledgment(doi, ack);
    } // - insertAcknowledgment

    /**
     * Insert each one of the given acknowledgments associating them to the given document
     * identifier.
     *
     * @param doi
     * @param acks
     * @throws DataAccessException
     */
    private void insertAcknowledgments(final String doi, final List<Acknowledgment> acks)
        throws DataAccessException
    {
        for (Acknowledgment ack : acks) {
            this.insertAcknowledgment(doi, ack);
        }
    } // - insertAcknowledgments

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.AckDAO#insertAckContexts(java.lang.Long, java.util.List)
     */
    @Override
    public void insertAckContexts(final Long ackID, final List<String> contexts)
        throws DataAccessException
    {
        this.ackDAO.insertAckContexts(ackID, contexts);
    } // - insertAckContexts

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.AckDAO#setAckCluster(edu.psu.citeseerx.domain.Acknowledgment,
     * java.lang.Long)
     */
    @Override
    public void setAckCluster(final Acknowledgment ack, final Long clusterID)
        throws DataAccessException
    {
        this.ackDAO.setAckCluster(ack, clusterID);
    } // - setAckCluster

    /*
     * (non-Javadoc)
     *
     * @see
     * edu.psu.citeseerx.dao2.AckDAO#updateAcknowledgment(edu.psu.citeseerx.domain.Acknowledgment)
     */
    @Override
    public void updateAcknowledgment(final Acknowledgment ack)
        throws DataAccessException
    {
        this.ackDAO.updateAcknowledgment(ack);
    } // - updateAcknowledgment

    // /////////////////////////////////////////////////////
    // Author DAO
    // /////////////////////////////////////////////////////

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.AuthorDAO#getDocAuthors(java.lang.String, boolean)
     */
    @Override
    public List<Author> getDocAuthors(final String docID, final boolean getSource)
        throws DataAccessException
    {
        return this.authDAO.getDocAuthors(docID, getSource);
    } // - getdocAuthors

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.AuthorDAO#insertAuthor(java.lang.String,
     * edu.psu.citeseerx.domain.Author)
     */
    @Override
    public void insertAuthor(final String docID, final Author auth)
        throws DataAccessException
    {
        this.authDAO.insertAuthor(docID, auth);
    } // - insertAuthor

    /**
     * Stores the given authors associating them to the given document identifier.
     *
     * @param doi
     * @param authors
     * @throws DataAccessException
     */
    private void insertAuthors(final String doi, final List<Author> authors)
        throws DataAccessException
    {
        for (Author author : authors) {
            this.insertAuthor(doi, author);
        }
    } // - insertAuthors

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.AuthorDAO#updateAuthor(edu.psu.citeseerx.domain.Author)
     */
    @Override
    public void updateAuthor(final Author auth)
        throws DataAccessException
    {
        this.authDAO.updateAuthor(auth);
    } // - updateAuthor

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.AuthorDAO#setAuthCluster(edu.psu.citeseerx.domain.Author,
     * java.lang.Long)
     */
    @Override
    public void setAuthCluster(final Author auth, final Long clusterID)
        throws DataAccessException
    {
        this.authDAO.setAuthCluster(auth, clusterID);
    } // - setAuthCluster

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.AuthorDAO#deleteAuthors(java.lang.String)
     */
    @Override
    public void deleteAuthors(final String docID)
        throws DataAccessException
    {
        this.authDAO.deleteAuthors(docID);
    } // - deleteAuthors

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.AuthorDAO#deleteAuthor(java.lang.Long)
     */
    @Override
    public void deleteAuthor(final Long authorID)
        throws DataAccessException
    {
        this.authDAO.deleteAuthor(authorID);
    } // - deleteAuthor

    // /////////////////////////////////////////////////////
    // Citation DAO
    // /////////////////////////////////////////////////////

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.CitationDAO#getCitations(java.lang.String, boolean)
     */
    @Override
    public List<Citation> getCitations(final String docID, final boolean getContexts)
        throws DataAccessException
    {
        return this.citeDAO.getCitations(docID, getContexts);
    } // - getCitations

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.CitationDAO#getCitations(long, int)
     */
    @Override
    public List<Citation> getCitations(final long startID, final int n)
        throws DataAccessException
    {
        return this.citeDAO.getCitations(startID, n);
    } // - getCitations

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.CitationDAO#getCitationsForCluster(java.lang.Long)
     */
    @Override
    public List<Citation> getCitationsForCluster(final Long clusterid)
        throws DataAccessException
    {
        return this.citeDAO.getCitationsForCluster(clusterid);
    } // - getCitationsForCluster

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.CitationDAO#getCitation(long)
     */
    @Override
    public Citation getCitation(final long id)
        throws DataAccessException
    {
        return this.citeDAO.getCitation(id);
    } // - getCitation

    /**
     * Stores all the given citations associating them to the given document
     *
     * @param DOI
     * @param citations
     * @throws DataAccessException
     */
    private void insertCitations(final String DOI, final List<Citation> citations)
        throws DataAccessException
    {
        for (Citation citation : citations) {
            this.citeDAO.insertCitation(DOI, citation);
        }
    } // - insertCitations

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.CitationDAO#insertCitation(java.lang.String,
     * edu.psu.citeseerx.domain.Citation)
     */
    @Override
    public void insertCitation(final String DOI, final Citation citation)
        throws DataAccessException
    {
        this.citeDAO.insertCitation(DOI, citation);
    } // - insertCitation

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.CitationDAO#getCiteContexts(java.lang.Long)
     */
    @Override
    public List<String> getCiteContexts(final Long citationID)
        throws DataAccessException
    {
        return this.citeDAO.getCiteContexts(citationID);
    } // - getCitationContexts

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.CitationDAO#insertCiteContexts(java.lang.Long, java.util.List)
     */
    @Override
    public void insertCiteContexts(final Long citationID, final List<String> contexts)
        throws DataAccessException
    {
        this.citeDAO.insertCiteContexts(citationID, contexts);
    } // - insertCitationContexts

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.CitationDAO#setCiteCluster(edu.psu.citeseerx.domain.Citation,
     * java.lang.Long)
     */
    @Override
    public void setCiteCluster(final Citation citation, final Long clusterID)
        throws DataAccessException
    {
        this.citeDAO.setCiteCluster(citation, clusterID);
    } // - setCitationCluster

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.CitationDAO#deleteCitations(java.lang.String)
     */
    @Override
    public void deleteCitations(final String DOI)
        throws DataAccessException
    {
        this.citeDAO.deleteCitations(DOI);
    } // - deleteCitations

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.CitationDAO#deleteCitation(java.lang.Long)
     */
    @Override
    public void deleteCitation(final Long citationID)
        throws DataAccessException
    {
        this.citeDAO.deleteCitation(citationID);
    } // - deleteCitation

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.CitationDAO#deleteCiteContexts(java.lang.Long)
     */
    @Override
    public void deleteCiteContexts(final Long citationID)
        throws DataAccessException
    {
        this.citeDAO.deleteCiteContexts(citationID);
    } // - deleteCitationContexts

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.CitationDAO#getNumberOfCitationsRecords()
     */
    @Override
    public Integer getNumberOfCitationsRecords()
        throws DataAccessException
    {
        return this.citeDAO.getNumberOfCitationsRecords();
    } // - getNumberOfCitationsRecords

    // /////////////////////////////////////////////////////
    // Document DAO
    // /////////////////////////////////////////////////////

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.DocumentDAO#getDocument(java.lang.String, boolean)
     */
    @Override
    public Document getDocument(final String docID, final boolean getSource)
        throws DataAccessException
    {
        return this.docDAO.getDocument(docID, getSource);
    } // - getDocument

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.DocumentDAO#updateDocument(edu.psu.citeseerx.domain.Document)
     */
    @Override
    public void updateDocument(final Document doc)
        throws DataAccessException
    {
        this.docDAO.updateDocument(doc);
    } // - updateDocument

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.DocumentDAO#insertDocument(edu.psu.citeseerx.domain.Document)
     */
    @Override
    public void insertDocument(final Document doc)
        throws DataAccessException
    {
        this.docDAO.insertDocument(doc);
    } // - insertDocument

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.DocumentDAO#insertDocumentSrc(edu.psu.citeseerx.domain.Document)
     */
    @Override
    public void insertDocumentSrc(final Document doc)
        throws DataAccessException
    {
        this.docDAO.insertDocumentSrc(doc);
    } // - insertDocumentSrc

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.DocumentDAO#setDocState(edu.psu.citeseerx.domain.Document, int)
     */
    @Override
    public void setDocState(final Document doc, final int toState)
        throws DataAccessException
    {
        this.docDAO.setDocState(doc, toState);
    } // - setDocState

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.DocumentDAO#setDocCluster(edu.psu.citeseerx.domain.Document,
     * java.lang.Long)
     */
    @Override
    public void setDocCluster(final Document doc, final Long clusterID)
        throws DataAccessException
    {
        this.docDAO.setDocCluster(doc, clusterID);
    } // - setDocCluster

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.DocumentDAO#setDocNcites(edu.psu.citeseerx.domain.Document, int)
     */
    @Override
    public void setDocNcites(final Document doc, final int ncites)
        throws DataAccessException
    {
        this.docDAO.setDocNcites(doc, ncites);
    } // - setNcites

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.DocumentDAO#getNumberOfDocumentRecords()
     */
    @Override
    public Integer getNumberOfDocumentRecords()
        throws DataAccessException
    {
        return this.docDAO.getNumberOfDocumentRecords();
    } // - getNumberOfDocumentRecords

    @Override
    public List<String> getAllDOIs()
        throws DataAccessException
    {
        return this.docDAO.getAllDOIs();
    }

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.DocumentDAO#getDOIs(java.lang.String, int)
     */
    @Override
    public List<String> getDOIs(final String start, final int amount)
        throws DataAccessException
    {
        return this.docDAO.getDOIs(start, amount);
    } // - getDOIs

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.DocumentDAO#getSetDOIs(java.util.Date, java.util.Date,
     * java.lang.String, int)
     */
    @Override
    public List<DOIInfo> getSetDOIs(final Date start, final Date end, final String prev,
            final int amount)
        throws DataAccessException
    {
        return this.docDAO.getSetDOIs(start, end, prev, amount);
    } // - getSetDOIs

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.DocumentDAO#getSetDOICount(java.util.Date, java.util.Date,
     * java.lang.String)
     */
    @Override
    public Integer getSetDOICount(final Date start, final Date end, final String prev)
        throws DataAccessException
    {
        return this.docDAO.getSetDOICount(start, end, prev);
    } // - getSetDOICount

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.DocumentDAO#getCrawledDOIs(java.util.Date, java.util.Date,
     * java.lang.String, int)
     */
    @Override
    public List<String> getCrawledDOIs(final Date start, final Date end, final String lastDOI,
            final int amount)
        throws DataAccessException
    {
        return this.docDAO.getCrawledDOIs(start, end, lastDOI, amount);
    } // - getCrawledDOIs

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.DocumentDAO#getLastDocuments(java.lang.String, int)
     */
    @Override
    public List<String> getLastDocuments(final String lastDOI, final int amount)
        throws DataAccessException
    {
        return this.docDAO.getLastDocuments(lastDOI, amount);
    } // - getLastDocuments

    // /////////////////////////////////////////////////////
    // File DAO
    // /////////////////////////////////////////////////////

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.FileDAO#insertChecksum(edu.psu.citeseerx.domain.CheckSum)
     */
    @Override
    public void insertChecksum(final CheckSum checksum)
    {
        this.fileDAO.insertChecksum(checksum);
    } // - insertChecksum

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.FileDAO#insertChecksums(java.lang.String, java.util.List)
     */
    @Override
    public void insertChecksums(final String doi, final List<CheckSum> checksums)
    {
        this.fileDAO.insertChecksums(doi, checksums);
    } // - insertChecksums

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.FileDAO#getChecksums(java.lang.String)
     */
    @Override
    public List<CheckSum> getChecksums(final String sha1)
        throws DataAccessException
    {
        return this.fileDAO.getChecksums(sha1);
    } // - getChecksums

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.FileDAO#deleteChecksums(java.lang.String)
     */
    @Override
    public void deleteChecksums(final String doi)
        throws DataAccessException
    {
        this.fileDAO.deleteChecksums(doi);
    }

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.FileDAO#updateChecksums(java.lang.String, java.util.List)
     */
    @Override
    public void updateChecksums(final String doi, final List<CheckSum> checksums)
        throws DataAccessException
    {
        this.fileDAO.updateChecksums(doi, checksums);
    } // - updateChecksums

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.FileDAO#getChecksumsForDocument(java.lang.String)
     */
    @Override
    public List<CheckSum> getChecksumsForDocument(final String doi)
    {
        return this.fileDAO.getChecksumsForDocument(doi);
    } // - getChecksumsForDocument

    // /////////////////////////////////////////////////////
    // Keyword DAO
    // /////////////////////////////////////////////////////

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.KeywordDAO#getKeywords(java.lang.String, boolean)
     */
    @Override
    public List<Keyword> getKeywords(final String doi, final boolean getSource)
        throws DataAccessException
    {
        return this.keywordDAO.getKeywords(doi, getSource);
    } // - getKeywords

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.KeywordDAO#insertKeyword(java.lang.String,
     * edu.psu.citeseerx.domain.Keyword)
     */
    @Override
    public void insertKeyword(final String docID, final Keyword keyword)
        throws DataAccessException
    {
        this.keywordDAO.insertKeyword(docID, keyword);
    } // - insertKeywords

    /**
     * Inserts the given keywords associating them to the given document identifier.
     *
     * @param docID
     * @param keywords
     * @throws DataAccessException
     */
    private void insertKeywords(final String docID, final List<Keyword> keywords)
        throws DataAccessException
    {
        for (Keyword keyword : keywords) {
            this.insertKeyword(docID, keyword);
        }
    } // - insertKeywords

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.KeywordDAO#updateKeyword(java.lang.String,
     * edu.psu.citeseerx.domain.Keyword)
     */
    @Override
    public void updateKeyword(final String docID, final Keyword keyword)
        throws DataAccessException
    {
        this.keywordDAO.updateKeyword(docID, keyword);
    } // - updateKeyword

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.KeywordDAO#deleteKeyword(java.lang.String,
     * edu.psu.citeseerx.domain.Keyword)
     */
    @Override
    public void deleteKeyword(final String docID, final Keyword keyword)
        throws DataAccessException
    {
        this.keywordDAO.deleteKeyword(docID, keyword);
    } // - deleteKeyword

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.KeywordDAO#deleteKeywords(java.lang.String)
     */
    @Override
    public void deleteKeywords(final String docID)
        throws DataAccessException
    {
        this.keywordDAO.deleteKeywords(docID);
    } // - deleteKeywords

    // /////////////////////////////////////////////////////
    // UserCorrection DAO
    // /////////////////////////////////////////////////////

    // /////////////////////////////////////////////////////
    // Version DAO
    // /////////////////////////////////////////////////////

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.VersionDAO#setVersion(java.lang.String, int)
     */
    @Override
    public void setVersion(final String doi, final int version)
        throws DataAccessException, IOException
    {
        Document doc = this.fileSysDAO.getDocVersion(doi, version);
        this.updateDocumentData(doc);
        this.versionDAO.deprecateVersionsAfter(doi, doc.getVersion());

    } // - setVersion

    /*
     * NOTE: Why this one is not in the interface?
     */
    public void setVersion(final String doi, final String name)
        throws DataAccessException, IOException
    {

        Document doc = this.fileSysDAO.getDocVersion(doi, name);
        this.versionDAO.deprecateVersionsAfter(doi, doc.getVersion());
        this.updateDocumentData(doc);

    } // - setVersion

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.VersionDAO#insertVersion(edu.psu.citeseerx.domain.Document)
     */
    @Override
    public boolean insertVersion(final Document doc)
        throws DataAccessException, IOException
    {

        this.versionDAO.insertVersion(doc);
        this.fileSysDAO.writeVersion(doc);
        return true;

    } // - createNewVersion

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.VersionDAO#setVersionName(java.lang.String, int,
     * java.lang.String)
     */
    @Override
    public void setVersionName(final String doi, final int version, final String name)
        throws DataAccessException
    {
        this.versionDAO.setVersionName(doi, version, name);
    } // - setVersionName

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.VersionDAO#setVersionSpam(java.lang.String, int, boolean)
     */
    @Override
    public void setVersionSpam(final String doi, final int version, final boolean isSpam)
        throws DataAccessException
    {
        this.versionDAO.setVersionSpam(doi, version, isSpam);
    } // - setSpam

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.VersionDAO#deprecateVersion(java.lang.String, int)
     */
    @Override
    public void deprecateVersion(final String doi, final int version)
        throws DataAccessException
    {
        this.versionDAO.deprecateVersion(doi, version);
    } // - deprecateVersion

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.VersionDAO#deprecateVersionsAfter(java.lang.String, int)
     */
    @Override
    public void deprecateVersionsAfter(final String doi, final int version)
        throws DataAccessException
    {
        this.versionDAO.deprecateVersionsAfter(doi, version);
    } // - deprecateVersionsAfter

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.VersionDAO#createNewVersion(edu.psu.citeseerx.domain.Document)
     */
    @Override
    public void createNewVersion(final Document doc)
        throws DataAccessException
    {
        this.versionDAO.createNewVersion(doc);
    } // - createNewVersion

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.VersionDAO#insertCorrection(java.lang.String, java.lang.String,
     * int)
     */
    @Override
    public void insertCorrection(final String userid, final String paperid, final int version)
        throws DataAccessException
    {
        this.versionDAO.insertCorrection(userid, paperid, version);
    } // - createNewVersion

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.VersionDAO#getCorrector(java.lang.String, int)
     */
    @Override
    public String getCorrector(final String paperid, final int version)
        throws DataAccessException
    {
        return this.versionDAO.getCorrector(paperid, version);
    } // - createNewVersion

    // /////////////////////////////////////////////////////
    // FileSys DAO
    // /////////////////////////////////////////////////////

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.FileSysDAO#getDocVersion(java.lang.String, int)
     */
    @Override
    public Document getDocVersion(final String doi, final int version)
        throws DataAccessException, IOException
    {
        return this.fileSysDAO.getDocVersion(doi, version);
    } // - getDocVersion

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.FileSysDAO#getDocVersion(java.lang.String, java.lang.String)
     */
    @Override
    public Document getDocVersion(final String doi, final String name)
        throws DataAccessException, IOException
    {
        return this.fileSysDAO.getDocVersion(doi, name);
    } // - getDocVersion

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.FileSysDAO#getFileInputStream(java.lang.String, java.lang.String,
     * java.lang.String)
     */
    @Override
    public FileInputStream getFileInputStream(final String doi, final String repID,
            final String type)
        throws IOException
    {
        return this.fileSysDAO.getFileInputStream(doi, repID, type);
    } // - getFileInputStream

    @Override
    public String getPath(final String doi, final String repID, final String type)
        throws UnknownRepositoryException
    {
        return this.fileSysDAO.getPath(doi, repID, type);
    }

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.FileSysDAO#getPdfReader(java.lang.String, java.lang.String)
     */
    @Override
    public PdfReader getPdfReader(final String doi, final String repID)
        throws IOException
    {
        return this.fileSysDAO.getPdfReader(doi, repID);
    } // - getPdfReader

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.FileSysDAO#writeXML(edu.psu.citeseerx.domain.Document)
     */
    @Override
    public void writeXML(final Document doc)
        throws IOException
    {
        this.fileSysDAO.writeXML(doc);
    } // - writeXML

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.FileSysDAO#writeVersion(edu.psu.citeseerx.domain.Document)
     */
    @Override
    public void writeVersion(final Document doc)
        throws IOException
    {
        this.fileSysDAO.writeVersion(doc);
    } // - writeVersion

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.FileSysDAO#getDocFromXML(java.lang.String, java.lang.String)
     */
    @Override
    public Document getDocFromXML(final String repID, final String relPath)
        throws IOException
    {
        return this.fileSysDAO.getDocFromXML(repID, relPath);
    } // - getDocFromXML

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.FileSysDAO#getFileTypes(java.lang.String, java.lang.String)
     */
    @Override
    public List<String> getFileTypes(final String doi, final String repID)
        throws IOException
    {
        return this.fileSysDAO.getFileTypes(doi, repID);
    } // - getFileTypes

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.FileSysDAO#getRepositoryID(java.lang.String)
     */
    @Override
    public String getRepositoryID(final String doi)
    {
        return this.fileSysDAO.getRepositoryID(doi);
    } // - getRepositoryID

    // /////////////////////////////////////////////////////
    // Hub DAO
    // /////////////////////////////////////////////////////

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.HubDAO#insertHub(edu.psu.citeseerx.domain.Hub)
     */
    @Override
    public long insertHub(final Hub hub)
        throws DataAccessException
    {
        Hub existingHub = this.hubDAO.getHub(hub.getUrl());
        if (existingHub == null)
            return this.hubDAO.insertHub(hub);
        else {
            this.hubDAO.updateHub(hub);
            return 0;
        }
    } // - insertHub

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.HubDAO#addHubMapping(edu.psu.citeseerx.domain.Hub,
     * java.lang.String, java.lang.String)
     */
    @Override
    public void addHubMapping(final Hub hub, final String url, final String doi)
        throws DataAccessException
    {
        this.hubDAO.addHubMapping(hub, url, doi);
    } // - addHubMapping

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.HubDAO#getHubs(java.lang.String)
     */
    @Override
    public List<Hub> getHubs(final String doi)
        throws DataAccessException
    {
        return this.hubDAO.getHubs(doi);
    } // - getHubs

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.HubDAO#getHubsForUrl(java.lang.String)
     */
    @Override
    public List<Hub> getHubsForUrl(final String url)
        throws DataAccessException
    {
        return this.hubDAO.getHubsForUrl(url);
    } // - getHubsForUrl

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.HubDAO#getHub(java.lang.String)
     */
    @Override
    public Hub getHub(final String url)
        throws DataAccessException
    {
        return this.hubDAO.getHub(url);
    } // - getHub

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.HubDAO#updateHub(edu.psu.citeseerx.domain.Hub)
     */
    @Override
    public void updateHub(final Hub hub)
        throws DataAccessException
    {
        this.hubDAO.updateHub(hub);
    } // - updateHub

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.HubDAO#insertHubMapping(long, long)
     */
    @Override
    public void insertHubMapping(final long urlID, final long hubID)
        throws DataAccessException
    {
        this.hubDAO.insertHubMapping(urlID, hubID);
    } // - insertHubMapping

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.HubDAO#insertUrl(java.lang.String, java.lang.String)
     */
    @Override
    public long insertUrl(final String doi, final String url)
    {
        return this.hubDAO.insertUrl(doi, url);
    } // - insertUrl

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.HubDAO#getUrls(java.lang.String)
     */
    @Override
    public List<String> getUrls(final String doi)
    {
        return this.hubDAO.getUrls(doi);
    } // - getUrls

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.HubDAO#getPaperIdsFromHubUrl(java.lang.String)
     */
    @Override
    public List<String> getPaperIdsFromHubUrl(final String hubUrl)
        throws DataAccessException
    {
        // TODO Auto-generated method stub
        return this.hubDAO.getPaperIdsFromHubUrl(hubUrl);
    } // - getPaperIdsFromHubUrl

    // /////////////////////////////////////////////////////
    // CiteChart DAO
    // /////////////////////////////////////////////////////

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.CiteChartDAO#checkChartUpdateRequired(java.lang.String)
     */
    @Override
    public boolean checkChartUpdateRequired(final String doi)
        throws DataAccessException
    {
        return this.citeChartDAO.checkChartUpdateRequired(doi);
    } // - chartUpdateRequired

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.CiteChartDAO#insertChartUpdate(java.lang.String, int,
     * java.lang.String)
     */
    @Override
    public void insertChartUpdate(final String doi, final int lastNcites, final String chartData)
        throws DataAccessException
    {
        this.citeChartDAO.insertChartUpdate(doi, lastNcites, chartData);
    } // - insertChartUpdate

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.CiteChartDAO#getCiteChartData(java.lang.String)
     */
    @Override
    public String getCiteChartData(final String doi)
        throws DataAccessException
    {
        return this.citeChartDAO.getCiteChartData(doi);
    } // - getCiteChartData

    // /////////////////////////////////////////////////////
    // Legacy ID DAO
    // /////////////////////////////////////////////////////

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.LegacyIDDAO#getNewID(int)
     */
    @Override
    public String getNewID(final int legacyID)
        throws DataAccessException
    {
        return this.legacyIDDAO.getNewID(legacyID);
    } // - getNewID

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.LegacyIDDAO#insertLegacyIDMapping(java.lang.String, int)
     */
    @Override
    public void insertLegacyIDMapping(final String csxID, final int legacyID)
        throws DataAccessException
    {
        this.legacyIDDAO.insertLegacyIDMapping(csxID, legacyID);
    } // - insertLegacyIDMapping

    // /////////////////////////////////////////////////////
    // Tag DAO
    // /////////////////////////////////////////////////////

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.TagDAO#addTag(java.lang.String, java.lang.String)
     */
    @Override
    public void addTag(final String paperid, final String tag)
        throws DataAccessException
    {
        this.tagDAO.addTag(paperid, tag);
    } // - addTag

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.TagDAO#deleteTag(java.lang.String, java.lang.String)
     */
    @Override
    public void deleteTag(final String paperid, final String tag)
        throws DataAccessException
    {
        this.tagDAO.deleteTag(paperid, tag);
    } // - deleteTag

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.TagDAO#getTags(java.lang.String)
     */
    @Override
    public List<Tag> getTags(final String paperid)
        throws DataAccessException
    {
        return this.tagDAO.getTags(paperid);
    } // - getTags

    // /////////////////////////////////////////////////////
    // Admin DAO
    // /////////////////////////////////////////////////////

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.AdminDAO#setBanner(java.lang.String)
     */
    @Override
    public void setBanner(final String banner)
        throws DataAccessException
    {
        this.adminDAO.setBanner(banner);
    } // - setBanner

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.AdminDAO#getBanner()
     */
    @Override
    public String getBanner()
        throws DataAccessException
    {
        return this.adminDAO.getBanner();
    } // - getBanner

    // /////////////////////////////////////////////////////
    // ExternalLink DAO
    // /////////////////////////////////////////////////////

    /*
     * (non-Javadoc)
     *
     * @see
     * edu.psu.citeseerx.dao2.ExternalLinkDAO#AddExternalLink(edu.psu.citeseerx.domain.ExternalLink)
     */
    @Override
    public void addExternalLink(final ExternalLink eLink)
        throws DataAccessException
    {
        this.externalLinkDAO.addExternalLink(eLink);
    } // - AddExternalLink

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.ExternalLinkDAO#addLinkType(edu.psu.citeseerx.domain.LinkType)
     */
    @Override
    public void addLinkType(final LinkType link)
        throws DataAccessException
    {
        this.externalLinkDAO.addLinkType(link);
    } // - addLinkType

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.ExternalLinkDAO#getExternalLiks(java.lang.String)
     */
    @Override
    public List<ExternalLink> getExternalLinks(final String doi)
        throws DataAccessException
    {
        return this.externalLinkDAO.getExternalLinks(doi);
    } // - getExternalLiks

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.ExternalLinkDAO#getLinkType(java.lang.String)
     */
    @Override
    public LinkType getLinkType(final String label)
        throws DataAccessException
    {
        return this.externalLinkDAO.getLinkType(label);
    } // - getLinkType

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.ExternalLinkDAO#getLinkTypes()
     */
    @Override
    public List<LinkType> getLinkTypes()
        throws DataAccessException
    {
        return this.externalLinkDAO.getLinkTypes();
    } // - getLinkTypes

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.ExternalLinkDAO#DeleteLinkType(edu.psu.citeseerx.domain.LinkType)
     */
    @Override
    public void deleteLinkType(final LinkType link)
        throws DataAccessException
    {
        this.externalLinkDAO.deleteLinkType(link);
    } // - DeleteLinkType

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.ExternalLinkDAO#UpdateLinkType(edu.psu.citeseerx.domain.LinkType,
     * java.lang.String)
     */
    @Override
    public void updateLinkType(final LinkType link, final String oldLabel)
        throws DataAccessException
    {
        this.externalLinkDAO.updateLinkType(link, oldLabel);
    } // - updateLinkType

    /*
     * (non-Javadoc)
     *
     * @see
     * edu.psu.citeseerx.dao2.ExternalLinkDAO#updateExternalLink(edu.psu.citeseerx.domain.ExternalLink
     * )
     */
    @Override
    public void updateExternalLink(final ExternalLink extLink)
        throws DataAccessException
    {
        this.externalLinkDAO.updateExternalLink(extLink);
    } // - updateExternalLink

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.ExternalLinkDAO#getPapersNoELink(java.lang.String,
     * java.lang.String, java.lang.Long)
     */
    @Override
    public List<String> getPapersNoELink(final String label, final String lastID, final Long amount)
    {
        return this.externalLinkDAO.getPapersNoELink(label, lastID, amount);
    } // - getPapersNoELink

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.ExternalLinkDAO#getExternalLinkExist(java.lang.String,
     * java.lang.String)
     */
    @Override
    public boolean getExternalLinkExist(final String label, final String doi)
        throws DataAccessException
    {
        return this.externalLinkDAO.getExternalLinkExist(label, doi);
    } // - getExternalLinkExist

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.ExternalLinkDAO#deleteExternalLink(java.lang.String,
     * java.lang.String)
     */
    @Override
    public void deleteExternalLink(final String doi, final String label)
        throws DataAccessException
    {
        this.externalLinkDAO.deleteExternalLink(doi, label);
    } // - deleteExternalLink

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.ExternalLinkDAO#getLink(java.lang.String, java.lang.String)
     */
    @Override
    public ExternalLink getLink(final String doi, final String label)
        throws DataAccessException
    {
        return this.externalLinkDAO.getLink(doi, label);
    } // - getLink

    // /////////////////////////////////////////////////////
    // Table DAO
    // /////////////////////////////////////////////////////

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.TableDAO#getTable(java.lang.Long)
     */
    @Override
    public Table getTable(final Long id)
        throws DataAccessException
    {
        return this.tableDAO.getTable(id);
    } // - getTable

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.TableDAO#countTable()
     */
    @Override
    public Integer countTable()
        throws DataAccessException
    {
        return this.tableDAO.countTable();
    } // - countTable

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.TableDAO#insertTable(edu.psu.citeseerx.domain.Table)
     */
    @Override
    public void insertTable(final Table tobj)
        throws DataAccessException
    {
        this.tableDAO.insertTable(tobj);
    } // - insertTable

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.TableDAO#deleteTable(java.lang.Long)
     */
    @Override
    public void deleteTable(final Long id)
        throws DataAccessException
    {
        this.tableDAO.deleteTable(id);
    } // - deleteTable

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.TableDAO#updateTableIndexTime()
     */
    @Override
    public void updateTableIndexTime()
        throws DataAccessException
    {
        this.tableDAO.updateTableIndexTime();
    } // - updateTableIndexTime

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.TableDAO#getUpdatedTables(java.sql.Date)
     */
    @Override
    public List<Table> getUpdatedTables(final java.sql.Date dt)
        throws DataAccessException
    {
        return this.tableDAO.getUpdatedTables(dt);
    } // - getUpdatedTables

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.TableDAO#getTables(java.lang.String, boolean)
     */
    @Override
    public List<Table> getTables(final String id, final boolean idtype)
        throws DataAccessException
    {
        return this.tableDAO.getTables(id, idtype);
    } // - getTables

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.TableDAO#lastTableIndexTime()
     */
    @Override
    public java.sql.Date lastTableIndexTime()
        throws DataAccessException
    {
        return this.tableDAO.lastTableIndexTime();
    } // -lastTableIndexTime

    // /////////////////////////////////////////////////////
    // UniqueAuthor DAO
    // /////////////////////////////////////////////////////

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.UniqueAuthorDAO#getAuthor(java.lang.String)
     */
    @Override
    public UniqueAuthor getAuthor(final String aid)
        throws DataAccessException
    {
        return this.uauthDAO.getAuthor(aid);
    } // - getdocAuthors

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.UniqueAuthorDAO#getAuthVarnames(java.lang.String)
     */
    @Override
    public List<String> getAuthVarnames(final String aid)
        throws DataAccessException
    {
        return this.uauthDAO.getAuthVarnames(aid);
    } // - getAuthVarnames

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.UniqueAuthorDAO#getAuthDocsOrdByCites(java.lang.String)
     */
    @Override
    public List<ThinDoc> getAuthDocsOrdByCites(final String aid)
        throws DataAccessException
    {
        return this.uauthDAO.getAuthDocsOrdByCites(aid);
    } // - getAuthDocs

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.UniqueAuthorDAO#getAuthDocsOrdByYear(java.lang.String)
     */
    @Override
    public List<ThinDoc> getAuthDocsOrdByYear(final String aid)
        throws DataAccessException
    {
        return this.uauthDAO.getAuthDocsOrdByYear(aid);
    } // - getAuthDocs

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.UniqueAuthorDAO#getAuthorRecords(java.lang.String,
     * java.util.List<java.lang.Integer>)
     */
    @Override
    public List<Integer> getAuthorRecords(final String aid)
        throws DataAccessException
    {
        return this.uauthDAO.getAuthorRecords(aid);
    } // - getAuthorsByPapers

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.UniqueAuthorDAO#getAuthorRecordsByPapers(java.lang.String,
     * java.util.List<java.lang.Integer>)
     */
    @Override
    public List<Integer> getAuthorRecordsByPapers(final String aid, final List<Integer> papers)
        throws DataAccessException
    {
        return this.uauthDAO.getAuthorRecordsByPapers(aid, papers);
    } // - getAuthorsByPapers

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.UniqueAuthorDAO#updateAuthNdocs(java.lang.String)
     */
    @Override
    public void updateAuthNdocs(final String aid)
        throws DataAccessException
    {
        this.uauthDAO.updateAuthNdocs(aid);
    } // - updateAuthNdocs

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.UniqueAuthorDAO#updateAuthNcites(java.lang.String)
     */
    @Override
    public void updateAuthNcites(final String aid)
        throws DataAccessException
    {
        this.uauthDAO.updateAuthNcites(aid);
    } // - updateAuthNcites

    /*
     * (non-Javadoc)
     *
     * @see
     * edu.psu.citeseerx.dao2.UniqueAuthorDAO#updateAuthInfo(edu.psu.citeseerx.domain.UniqueAuthor)
     */
    @Override
    public void updateAuthInfo(final UniqueAuthor uauth)
        throws DataAccessException
    {
        this.uauthDAO.updateAuthInfo(uauth);
    } // - updateAuthInfo

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.UniqueAuthorDAO#removeAuthor(java.lang.String)
     */
    @Override
    public void removeAuthor(final String aid)
        throws DataAccessException
    {
        this.uauthDAO.removeAuthor(aid);
    } // - removeAuthor

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.UniqueAuthorDAO#moveAuthorRecord(java.lang.String,
     * java.util.List<java.lang.String>)
     */
    @Override
    public void moveAuthorRecords(final String target_aid, final List<Integer> author_records)
        throws DataAccessException
    {
        this.uauthDAO.moveAuthorRecords(target_aid, author_records);
    } // - moveAuthorRecords

    // /////////////////////////////////////////////////////
    // UniqueAuthorVersion DAO
    // /////////////////////////////////////////////////////

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.UniqueAuthorVersionDAO#updateUauthorInfo(java.lang.String,
     * java.lang.String, java.lang.String, java.lang.String)
     */
    @Override
    public void updateUauthorInfo(final String userid, final String aid, final String new_canname,
            final String new_affil)
        throws DataAccessException
    {
        this.uauthVersionDAO.updateUauthorInfo(userid, aid, new_canname, new_affil);
    } // - updateUauthorInfo

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.UniqueAuthorVersionDAO#mergeUauthors(java.lang.String,
     * java.lang.String, java.lang.String)
     */
    @Override
    public void mergeUauthors(final String userid, final String aid1, final String aid2)
        throws DataAccessException
    {
        this.uauthVersionDAO.mergeUauthors(userid, aid1, aid2);
    } // - mergeUauthors

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.UniqueAuthorVersionDAO#removeUauthorPapers(java.lang.String,
     * java.lang.String, java.util.List<java.lang.Integer>)
     */
    @Override
    public void removeUauthorPapers(final String userid, final String aid,
            final List<Integer> papers)
        throws DataAccessException
    {
        this.uauthVersionDAO.removeUauthorPapers(userid, aid, papers);
    } // - removeUauthorPapers

    // /////////////////////////////////////////////////////
    // AlgorithmDAO
    // /////////////////////////////////////////////////////

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.AlgorithmDAO#countAlgorithm()
     */
    @Override
    public Integer countAlgorithm()
        throws DataAccessException
    {
        return this.algorithmDAO.countAlgorithm();
    } // - countAlgorithm

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.AlgorithmDAO#getAlgorithm(java.lang.String)
     */
    @Override
    public Algorithm getAlgorithm(final long id)
        throws DataAccessException
    {
        return this.algorithmDAO.getAlgorithm(id);
    } // - getAlgorithm

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.AlgorithmDAO#getUpdatedAlgorithms(java.util.Date)
     */
    @Override
    public List<Algorithm> getUpdatedAlgorithms(final Date dt)
        throws DataAccessException
    {
        return this.algorithmDAO.getUpdatedAlgorithms(dt);
    } // - getUpdatedAlgorithms

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.AlgorithmDAO#insertAlgorithm(edu.psu.citeseerx.domain.Algorithm)
     */
    @Override
    public void insertAlgorithm(final Algorithm oneAlgorithm)
        throws DataAccessException
    {
        this.algorithmDAO.insertAlgorithm(oneAlgorithm);
    } // - insertAlgorithm

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.AlgorithmDAO#lastAlgorithmIndexTime()
     */
    @Override
    public Date lastAlgorithmIndexTime()
        throws DataAccessException
    {
        return this.algorithmDAO.lastAlgorithmIndexTime();
    } // - lastAlgorithmIndexTime

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.AlgorithmDAO#updateAlgorithmIndexTime()
     */
    @Override
    public void updateAlgorithmIndexTime()
        throws DataAccessException
    {
        this.algorithmDAO.updateAlgorithmIndexTime();
    } // - updateAlgorithmIndexTime

    // /////////////////////////////////////////////////////
    // GeneralStatistics
    // /////////////////////////////////////////////////////

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.GeneralStatistics#getAuthorsInCollection()
     */
    @Override
    public long getAuthorsInCollection()
    {
        return this.generalStatistics.getAuthorsInCollection();
    } // - getAuthorsInCollection

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.GeneralStatistics#getCitationsInCollection()
     */
    @Override
    public long getCitationsInCollection()
    {
        return this.generalStatistics.getCitationsInCollection();
    } // - getCitationsInCollection

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.GeneralStatistics#getDocumentsInCollection()
     */
    @Override
    public long getDocumentsInCollection()
    {
        return this.generalStatistics.getDocumentsInCollection();
    } // - getDocumentsInCollection

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.GeneralStatistics#getPublicDocumentsInCollection()
     */
    @Override
    public long getPublicDocumentsInCollection()
    {
        return this.generalStatistics.getPublicDocumentsInCollection();
    } // - getPublicDocumentsInCollection

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.GeneralStatistics#getDisambiguatedAuthorsInCollection()
     */
    @Override
    public long getDisambiguatedAuthorsInCollection()
    {
        return this.generalStatistics.getDisambiguatedAuthorsInCollection();
    } // - getDisambiguatedAuthorsInCollection

    @Override
    public long getUniqueAuthorsInCollection()
    {
        return this.generalStatistics.getUniqueAuthorsInCollection();
    } // - getUniqueAuthorsInCollection

    @Override
    public long getNumberofUniquePublicDocuments()
    {
        return this.generalStatistics.getNumberofUniquePublicDocuments();
    } // - getNumberofUniquePublicDocuments

    @Override
    public long getUniqueEntitiesInCollection()
    {
        return this.generalStatistics.getUniqueEntitiesInCollection();
    } // - getUniqueEntitiesInCollection

    // /////////////////////////////////////////////////////
    // RedirectPDFDAO
    // /////////////////////////////////////////////////////
    @Override
    public PDFRedirect getPDFRedirect(final String doi)
        throws DataAccessException
    {
        return this.redirectPDFDAO.getPDFRedirect(doi);
    }

    @Override
    public void insertPDFRedirect(final PDFRedirect pdfredirect)
        throws DataAccessException
    {

        this.redirectPDFDAO.insertPDFRedirect(pdfredirect);
    }

    @Override
    public void updatePDFRedirect(final String doi, final PDFRedirect pdfredirect)
        throws DataAccessException
    {
        this.redirectPDFDAO.updatePDFRedirect(doi, pdfredirect);

    }

    @Override
    public void updatePDFRedirectTemplate(final String label, final String urltemplate)
        throws DataAccessException
    {
        this.redirectPDFDAO.updatePDFRedirectTemplate(label, urltemplate);
    }

} // - class CSXDAOImpl
