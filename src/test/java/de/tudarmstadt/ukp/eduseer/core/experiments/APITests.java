/*******************************************************************************
 * Copyright 2013
 * Ubiquitous Knowledge Processing (UKP) Lab
 * Technische Universität Darmstadt
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package de.tudarmstadt.ukp.eduseer.core.experiments;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import javax.annotation.Resource;
import javax.sql.DataSource;

import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractTransactionalJUnit4SpringContextTests;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.transaction.TransactionConfiguration;

import edu.psu.citeseerx.dao2.logic.CSXDAO;
import edu.psu.citeseerx.domain.Author;
import edu.psu.citeseerx.domain.Citation;
import edu.psu.citeseerx.domain.Document;

/**
 * This is a collection of tests that demonstrate how to use the SeerSuite API.
 *
 * @author Roland Kluge
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:conf/applicationContext-csx-jdbc.xml" })
@TransactionConfiguration(transactionManager = "csxTxManager", defaultRollback = true)
public class APITests
    extends AbstractTransactionalJUnit4SpringContextTests
{
    @Autowired
    public CSXDAO csxDAO;

    @Override
    @Resource(name = "csxDataSource")
    public void setDataSource(final DataSource dataSource)
    {
        this.simpleJdbcTemplate = new SimpleJdbcTemplate(dataSource);
    }

    /**
     * Demonstrates how to iterate over all DOIs.
     */
    @Test
    public void testGetAllDOIs()
        throws Exception
    {
        final List<String> alldois = this.csxDAO.getAllDOIs();
        System.out.println("DOIs in database: " + alldois);
    }

    /**
     * Demonstrates how to access the fulltext of a document. Note that "available types" are the
     * supported document types from the end user perspektive (such as PDF, PS,...)
     */
    @Test
    public void testGetFullText()
        throws Exception
    {
        final List<Document> docs = this.getDocumentsByPattern(".*Eltern helfen Eltern.*");
        for (final Document doc : docs) {

            System.out.println(doc.getDatum(Document.TITLE_KEY));

            final String doi = doc.getDatum(Document.DOI_KEY);
            final String repID = this.csxDAO.getRepositoryID(doi);
            final List<String> availableFiletypes = this.csxDAO.getFileTypes(doi, repID);

            System.out.println("Available types: " + availableFiletypes);

            System.out.println();
            FileInputStream inputStream = null;

            try {
                inputStream = this.csxDAO.getFileInputStream(doi, repID, "xml");
                final Scanner scanner = new Scanner(inputStream);

                final int maxLines = 30;
                int lines = 0;
                while (scanner.hasNextLine() && lines < maxLines) {
                    System.out.println(scanner.nextLine());
                    ++lines;
                }
            }
            catch (final IOException e) {
                e.printStackTrace();
            }
            finally {
                IOUtils.closeQuietly(inputStream);
            }
        }

    }

    /**
     * Demonstrates how to obtain the file system path to a document's file.
     */
    @Test
    public void testGetPathToDocument()
        throws Exception
    {
        final List<Document> docs = this.getDocumentsByPattern(".*Eltern helfen Eltern.*");
        for (final Document doc : docs) {

            final String doi = doc.getDatum(Document.DOI_KEY);
            final String repID = this.csxDAO.getRepositoryID(doi);
            final String path = this.csxDAO.getPath(doi, repID, "pdf");
            System.out.println(String.format("%s: %s", doi, path));
        }
    }

    /**
     * Demonstrates how to extract the authors of a paper
     */
    @Test
    public void testGetAuthors()
        throws Exception
    {
        final List<Document> docs = this.getDocumentsByPattern(".*Eltern helfen Eltern.*");
        for (final Document doc : docs) {
            System.out.println(doc.getDatum(Document.TITLE_KEY));

            System.out.println("Authors: ");
            for (final Author author : doc.getAuthors()) {
                final String name = author.getDatum(Author.NAME_KEY);
                final String mail = author.getDatum(Author.EMAIL_KEY);
                System.out.println(String.format("\t%s (Mail: %s)", name, mail));
            }
        }
    }

    /**
     * Demonstrates how to extract the citations of a paper
     */
    @Test
    public void testGetCitations()
        throws Exception
    {
        final List<Document> docs = this
                .getDocumentsByPattern(".*Erziehungskompetenzen bei jugendlichen Müttern.*");
        for (final Document doc : docs) {
            System.out.println(doc.getDatum(Document.TITLE_KEY));

            System.out.print("Citations: ");
            for (final Citation citation : doc.getCitations()) {
                final List<String> authors = citation.getAuthorNames();
                final String title = citation.getDatum(Citation.TITLE_KEY);
                final String year = citation.getDatum(Citation.YEAR_KEY);
                System.out.println(String.format("\t\"%s\" by %s(%s)", title, authors, year));
            }
        }
    }

    /**
     * Returns all documents that have a title matching the given pattern.
     *
     * @param pattern
     *            the pattern to match the document title against
     * @return all matching documents
     */
    private List<Document> getDocumentsByPattern(final String pattern)
    {
        final List<Document> docs = new ArrayList<Document>();

        final List<String> alldois = this.csxDAO.getAllDOIs();
        for (final String doi : alldois) {

            /*
             * During filtering, we are only interested in the document's title. Loading all
             * document metadata would take significantly longer!
             */
            final Document doc = this.csxDAO.getDocumentFromDB(doi);

            final String title = doc.getDatum(Document.TITLE_KEY);
            if (title != null && title.matches(pattern)) {

                final boolean getCitations = true;
                final boolean getContexts = true;
                final boolean getSource = true;
                final boolean getKeywords = true;
                final boolean getTags = true;
                final boolean getAcknowledgements = true;
                final Document fullDoc = this.csxDAO.getDocumentFromDB(doi, getCitations,
                        getContexts, getSource, getAcknowledgements, getKeywords, getTags);
                docs.add(fullDoc);

            }
        }

        return docs;
    }
}
