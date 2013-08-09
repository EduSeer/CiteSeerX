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
 *
 * Copyright 2013
 * Ubiquitous Knowledge Processing (UKP) Lab
 * Technische Universität Darmstadt
 */
package edu.psu.citeseerx.dao2;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;
import java.util.List;

import javax.sql.DataSource;

import org.springframework.context.ApplicationContextException;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.SqlParameter;
import org.springframework.jdbc.core.support.JdbcDaoSupport;
import org.springframework.jdbc.object.MappingSqlQuery;
import org.springframework.jdbc.object.SqlUpdate;

import de.tudarmstadt.ukp.eduseer.core.dao.DAOConstants;
import edu.psu.citeseerx.domain.DOIInfo;
import edu.psu.citeseerx.domain.Document;
import edu.psu.citeseerx.domain.DocumentFileInfo;

/**
 * DocumentDAO Implementation using MySQL as a persistent storage
 *
 * @author Isaac Councill
 * @version $Rev$ $Date$
 */
public class DocumentDAOImpl
    extends JdbcDaoSupport
    implements DocumentDAO
{

    private GetDoc getDoc;
    private GetDocSrc getDocSrc;
    private InsertDoc insertDoc;
    private InsertDocSrc insertDocSrc;
    private UpdateDoc updateDoc;
    private UpdateDocSrc updateDocSrc;
    private SetState setState;
    private SetCluster setCluster;
    private SetNcites setNcites;
    private CountDocs countDocs;
    private GetDOIs getDOIs;
    private GetAllDOIs getAllDOIs;
    private GetSetDOIs getSetDOIs;
    private GetSetDOICount getSetDOICount;
    private GetCrawledDOIs getCrawledDOIs;
    private GetLatestDocuments getLatestDocuments;

    /*
     * (non-Javadoc)
     *
     * @see org.springframework.dao.support.DaoSupport#initDao()
     */
    @Override
    protected void initDao()
        throws ApplicationContextException
    {
        this.initMappingSqlQueries();
    } // - initDao

    protected void initMappingSqlQueries()
        throws ApplicationContextException
    {

        this.getDoc = new GetDoc(this.getDataSource());
        this.getDocSrc = new GetDocSrc(this.getDataSource());
        this.insertDoc = new InsertDoc(this.getDataSource());
        this.insertDocSrc = new InsertDocSrc(this.getDataSource());
        this.updateDoc = new UpdateDoc(this.getDataSource());
        this.updateDocSrc = new UpdateDocSrc(this.getDataSource());
        this.setState = new SetState(this.getDataSource());
        this.setCluster = new SetCluster(this.getDataSource());
        this.setNcites = new SetNcites(this.getDataSource());
        this.countDocs = new CountDocs(this.getDataSource());
        this.getDOIs = new GetDOIs(this.getDataSource());
        this.getAllDOIs = new GetAllDOIs(this.getDataSource());
        this.getSetDOIs = new GetSetDOIs(this.getDataSource());
        this.getSetDOICount = new GetSetDOICount(this.getDataSource());
        this.getCrawledDOIs = new GetCrawledDOIs(this.getDataSource());
        this.getLatestDocuments = new GetLatestDocuments(this.getDataSource());
    } // - initMappingSqlQueries

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.DocumentDAO#getDocument(java.lang.String, boolean)
     */
    @Override
    public Document getDocument(final String doi, final boolean getSource)
        throws DataAccessException
    {
        Document doc = this.getDoc.run(doi);
        if (doc == null)
            return null;
        if (getSource) {
            Document srcDoc = this.getDocSrc.run(doi);
            if (srcDoc == null) {
                System.err.println("WARNING: Null Source Doc for " + doi);
                srcDoc = new Document(); // Just in case...
            }
            doc.setSource(Document.TITLE_KEY, srcDoc.getSource(Document.TITLE_KEY));
            doc.setSource(Document.ABSTRACT_KEY, srcDoc.getSource(Document.ABSTRACT_KEY));
            doc.setSource(Document.YEAR_KEY, srcDoc.getSource(Document.YEAR_KEY));
            doc.setSource(Document.VENUE_KEY, srcDoc.getSource(Document.VENUE_KEY));
            doc.setSource(Document.VEN_TYPE_KEY, srcDoc.getSource(Document.VEN_TYPE_KEY));
            doc.setSource(Document.PAGES_KEY, srcDoc.getSource(Document.PAGES_KEY));
            doc.setSource(Document.VOL_KEY, srcDoc.getSource(Document.VOL_KEY));
            doc.setSource(Document.NUM_KEY, srcDoc.getSource(Document.NUM_KEY));
            doc.setSource(Document.PUBLISHER_KEY, srcDoc.getSource(Document.PUBLISHER_KEY));
            doc.setSource(Document.PUBADDR_KEY, srcDoc.getSource(Document.PUBADDR_KEY));
            doc.setSource(Document.TECH_KEY, srcDoc.getSource(Document.TECH_KEY));
            doc.setSource(Document.CITES_KEY, srcDoc.getSource(Document.CITES_KEY));
        }
        return doc;

    } // - getDocument

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.DocumentDAO#insertDocument(edu.psu.citeseerx.domain.Document)
     */
    @Override
    public void insertDocument(final Document doc)
        throws DataAccessException
    {
        this.insertDoc.run(doc);
    } // - insertDocument

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.DocumentDAO#insertDocumentSrc(edu.psu.citeseerx.domain.Document)
     */
    @Override
    public void insertDocumentSrc(final Document doc)
    {
        if (doc.hasSourceData()) {
            this.insertDocSrc.run(doc);
        }
    } // - insertDocumentSrc

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.DocumentDAO#updateDocument(edu.psu.citeseerx.domain.Document)
     */
    @Override
    public void updateDocument(final Document doc)
        throws DataAccessException
    {
        this.updateDoc.run(doc);
        if (doc.hasSourceData()) {
            this.updateDocSrc.run(doc);
        }
    } // - updateDocument

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.DocumentDAO#setDocPublic(edu.psu.citeseerx.domain.Document, int)
     */
    @Override
    public void setDocState(final Document doc, final int toState)
        throws DataAccessException
    {
        this.setState.run(doc, toState);
    } // - setPublic

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
        this.setCluster.run(doc, clusterID);
    } // - setCluster

    /*
     * (non-Javadoc)
     *
     * @see edu.psu.citeseerx.dao2.DocumentDAO#setDocNcites(edu.psu.citeseerx.domain.Document, int)
     */
    @Override
    public void setDocNcites(final Document doc, final int ncites)
        throws DataAccessException
    {
        this.setNcites.run(doc, ncites);
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
        return this.countDocs.run();
    } // - getNumberOfDocumentRecords

    @Override
    public List<String> getAllDOIs()
        throws DataAccessException
    {
        return this.getAllDOIs.run();
    }

    @Override
    public List<String> getDOIs(final String start, final int amount)
        throws DataAccessException
    {
        return this.getDOIs.run(start, amount);
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
        return this.getSetDOIs.run(start, end, prev, amount);
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
        return this.getSetDOICount.run(start, end, prev);
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
        return this.getCrawledDOIs.run(start, end, lastDOI, amount);
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
        return this.getLatestDocuments.run(lastDOI, amount);
    } // - getLastDocuments

    private static final String DEF_GET_DOC_QUERY = "select id, version, cluster, title, abstract, year, venue, "
            + "venueType, pages, volume, number, publisher, pubAddress, tech, "
            + "public, ncites, versionName, crawlDate, repositoryID, "
            + "conversionTrace, selfCites, versionTime from papers where id=?";

    private class GetDoc
        extends MappingSqlQuery
    {

        public GetDoc(final DataSource dataSource)
        {
            this.setDataSource(dataSource);
            this.setSql(DEF_GET_DOC_QUERY);
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.compile();
        } // - GetDoc.GetDoc

        @Override
        public Document mapRow(final ResultSet rs, final int rowNum)
            throws SQLException
        {
            Document doc = new Document();
            doc.setDatum(Document.DOI_KEY, rs.getString("id"));
            doc.setVersion(rs.getInt("version"));
            doc.setClusterID(rs.getLong("cluster"));
            doc.setDatum(Document.TITLE_KEY, rs.getString("title"));
            doc.setDatum(Document.ABSTRACT_KEY, rs.getString("abstract"));
            doc.setDatum(Document.YEAR_KEY, rs.getString("year"));
            doc.setDatum(Document.VENUE_KEY, rs.getString("venue"));
            doc.setDatum(Document.VEN_TYPE_KEY, rs.getString("venueType"));
            doc.setDatum(Document.PAGES_KEY, rs.getString("pages"));
            doc.setDatum(Document.VOL_KEY, rs.getString("volume"));
            doc.setDatum(Document.NUM_KEY, rs.getString("number"));
            doc.setDatum(Document.PUBLISHER_KEY, rs.getString("publisher"));
            doc.setDatum(Document.PUBADDR_KEY, rs.getString("pubAddress"));
            doc.setDatum(Document.TECH_KEY, rs.getString("tech"));
            doc.setNcites(rs.getInt("ncites"));
            doc.setSelfCites(rs.getInt("selfCites"));
            doc.setVersionName(rs.getString("versionName"));
            doc.setState(rs.getInt("public"));
            /*
             * if (rs.getBoolean("public")) { doc.setState(DocumentProperties.IS_PUBLIC); } else{
             * doc.setState(DocumentProperties.LOGICAL_DELETE); }
             */
            doc.setVersionTime(new Date(rs.getTimestamp("versionTime").getTime()));

            DocumentFileInfo finfo = new DocumentFileInfo();
            finfo.setDatum(DocumentFileInfo.CRAWL_DATE_KEY,
                    DateFormat.getDateInstance().format(rs.getTimestamp("crawlDate")));
            finfo.setDatum(DocumentFileInfo.REP_ID_KEY, rs.getString("repositoryID"));
            finfo.setDatum(DocumentFileInfo.CONV_TRACE_KEY, rs.getString("conversionTrace"));
            doc.setFileInfo(finfo);

            return doc;
        } // - GetDoc.mapRow

        public Document run(final String doi)
        {
            List<Document> list = this.execute(doi);
            if (list.isEmpty())
                return null;
            else
                return list.get(0);
        } // - GetDoc.run

    } // - class GetDoc

    private static final String DEF_GET_DOC_SRC_QUERY = "select title, abstract, year, venue, venueType, pages, volume, "
            + "number, publisher, pubAddress, tech, citations from "
            + "papers_versionShadow where id=?";

    private class GetDocSrc
        extends MappingSqlQuery
    {

        public GetDocSrc(final DataSource dataSource)
        {
            this.setDataSource(dataSource);
            this.setSql(DEF_GET_DOC_SRC_QUERY);
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.compile();
        } // - GetDocSrc.GetDocSrc

        @Override
        public Document mapRow(final ResultSet rs, final int rowNum)
            throws SQLException
        {
            Document doc = new Document();
            doc.setSource(Document.TITLE_KEY, rs.getString("title"));
            doc.setSource(Document.ABSTRACT_KEY, rs.getString("abstract"));
            doc.setSource(Document.YEAR_KEY, rs.getString("year"));
            doc.setSource(Document.VENUE_KEY, rs.getString("venue"));
            doc.setSource(Document.VEN_TYPE_KEY, rs.getString("venueType"));
            doc.setSource(Document.PAGES_KEY, rs.getString("pages"));
            doc.setSource(Document.VOL_KEY, rs.getString("volume"));
            doc.setSource(Document.NUM_KEY, rs.getString("number"));
            doc.setSource(Document.PUBLISHER_KEY, rs.getString("publisher"));
            doc.setSource(Document.PUBADDR_KEY, rs.getString("pubAddress"));
            doc.setSource(Document.TECH_KEY, rs.getString("tech"));
            doc.setSource(Document.CITES_KEY, rs.getString("citations"));
            return doc;
        }

        public Document run(final String doi)
        {
            List<Document> list = this.execute(doi);
            if (list.isEmpty())
                return null;
            else
                return list.get(0);
        } // - GetDocSrc.run

    } // - class GetDocSrc

    /*
     * id, version, cluster, title, abstract, year, venue, venueType, pages, volume, number,
     * publisher, pubAddress, tech, public, size, versionName, crawlDate, repositoryID,
     * conversionTrace, selfCites, versionTime
     */
    private static final String DEF_INSERT_DOC_QUERY = "insert into papers values (?, ?, ?, ?, ?, ?, ?, ?, ?,"
            + " ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)";

    private class InsertDoc
        extends SqlUpdate
    {

        public InsertDoc(final DataSource dataSource)
        {
            this.setDataSource(dataSource);
            this.setSql(DEF_INSERT_DOC_QUERY);
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.declareParameter(new SqlParameter(Types.INTEGER));
            this.declareParameter(new SqlParameter(Types.BIGINT));
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.declareParameter(new SqlParameter(Types.INTEGER));
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.declareParameter(new SqlParameter(Types.INTEGER));
            this.declareParameter(new SqlParameter(Types.INTEGER));
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.declareParameter(new SqlParameter(Types.TINYINT));
            this.declareParameter(new SqlParameter(Types.INTEGER));
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.declareParameter(new SqlParameter(Types.TIMESTAMP));
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.declareParameter(new SqlParameter(Types.INTEGER));
            this.compile();
        } // - InsertDoc.InsertDoc

        public int run(final Document doc)
        {
            Integer year = null;
            try {
                year = Integer.parseInt(doc.getDatum(Document.YEAR_KEY));
            }
            catch (Exception e) {
            }
            Integer vol = null;
            try {
                vol = Integer.parseInt(doc.getDatum(Document.VOL_KEY));
            }
            catch (Exception e) {
            }
            Integer num = null;
            try {
                num = Integer.parseInt(doc.getDatum(Document.NUM_KEY));
            }
            catch (Exception e) {
            }

            DocumentFileInfo finfo = doc.getFileInfo();
            java.util.Date crawlDate = null;
            try {
                crawlDate = DateFormat.getDateInstance().parse(
                        finfo.getDatum(DocumentFileInfo.CRAWL_DATE_KEY));
            }
            catch (ParseException e) {
                e.printStackTrace();
            }
            catch (NullPointerException e) {
                /* that's ok - we'll use current_timestamp */
                crawlDate = new java.util.Date(System.currentTimeMillis());
            }

            Object[] params = new Object[] { doc.getDatum(Document.DOI_KEY), doc.getVersion(),
                    doc.getClusterID(), doc.getDatum(Document.TITLE_KEY),
                    doc.getDatum(Document.ABSTRACT_KEY), year, doc.getDatum(Document.VENUE_KEY),
                    doc.getDatum(Document.VEN_TYPE_KEY), doc.getDatum(Document.PAGES_KEY), vol,
                    num, doc.getDatum(Document.PUBLISHER_KEY), doc.getDatum(Document.PUBADDR_KEY),
                    doc.getDatum(Document.TECH_KEY), doc.getState(), doc.getNcites(),
                    doc.getVersionName(), new Timestamp(crawlDate.getTime()),
                    finfo.getDatum(DocumentFileInfo.REP_ID_KEY),
                    finfo.getDatum(DocumentFileInfo.CONV_TRACE_KEY), doc.getSelfCites() };
            return this.update(params);
        } // - InsertDoc.run

    } // - class InsertDoc

    /*
     * id, title, abstract, year, venue, venueType, pages, volume, number, publisher, pubAddress,
     * tech, cites
     */
    private static final String DEF_INSERT_DOC_SRC_QUERY = "insert into papers_versionShadow values (?, ?, ?, ?, ?, ?, ?, ?, "
            + "?, ?, ?, ?, ?)";

    private class InsertDocSrc
        extends SqlUpdate
    {

        public InsertDocSrc(final DataSource dataSource)
        {
            this.setDataSource(dataSource);
            this.setSql(DEF_INSERT_DOC_SRC_QUERY);
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.compile();
        } // - InsertDocSrc.InsertDocSrc

        public int run(final Document doc)
        {
            Object[] params = new Object[] { doc.getDatum(Document.DOI_KEY),
                    doc.getSource(Document.TITLE_KEY), doc.getSource(Document.ABSTRACT_KEY),
                    doc.getSource(Document.YEAR_KEY), doc.getSource(Document.VENUE_KEY),
                    doc.getSource(Document.VEN_TYPE_KEY), doc.getSource(Document.PAGES_KEY),
                    doc.getSource(Document.VOL_KEY), doc.getSource(Document.NUM_KEY),
                    doc.getSource(Document.PUBLISHER_KEY), doc.getSource(Document.PUBADDR_KEY),
                    doc.getSource(Document.TECH_KEY), doc.getSource(Document.CITES_KEY) };
            return this.update(params);
        } // - InsertDocSrc

    } // - class InsertDocSrc

    private static final String DEF_UPDATE_DOC_QUERY = "update papers set version=?, title=?, abstract=?, "
            + "year=?, venue=?, venueType=?, pages=?, volume=?, number=?, "
            + "publisher=?, pubAddress=?, tech=?, public=?, "
            + "versionName=?, crawlDate=?, repositoryID=?, conversionTrace=? " + "where id=?";

    private class UpdateDoc
        extends SqlUpdate
    {

        public UpdateDoc(final DataSource dataSource)
        {
            this.setDataSource(dataSource);
            this.setSql(DEF_UPDATE_DOC_QUERY);
            this.declareParameter(new SqlParameter(Types.INTEGER));
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.declareParameter(new SqlParameter(Types.BLOB));
            this.declareParameter(new SqlParameter(Types.INTEGER));
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.declareParameter(new SqlParameter(Types.INTEGER));
            this.declareParameter(new SqlParameter(Types.INTEGER));
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.declareParameter(new SqlParameter(Types.TINYINT));
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.declareParameter(new SqlParameter(Types.TIMESTAMP));
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.compile();
        } // - UpdateDoc.UpdateDoc

        public int run(final Document doc)
        {
            Integer year = null;
            try {
                year = Integer.parseInt(doc.getDatum(Document.YEAR_KEY));
            }
            catch (Exception e) {
            }
            Integer vol = null;
            try {
                vol = Integer.parseInt(doc.getDatum(Document.VOL_KEY));
            }
            catch (Exception e) {
            }
            Integer num = null;
            try {
                num = Integer.parseInt(doc.getDatum(Document.NUM_KEY));
            }
            catch (Exception e) {
            }

            DocumentFileInfo finfo = doc.getFileInfo();
            java.util.Date crawlDate = null;
            try {
                crawlDate = DateFormat.getDateInstance().parse(
                        finfo.getDatum(DocumentFileInfo.CRAWL_DATE_KEY));
            }
            catch (ParseException e) {
                e.printStackTrace();
            }
            catch (NullPointerException e) {
                /* that's ok - we'll use current_timestamp */
                crawlDate = new java.util.Date(System.currentTimeMillis());
            }

            Object[] params = new Object[] { doc.getVersion(), doc.getDatum(Document.TITLE_KEY),
                    doc.getDatum(Document.ABSTRACT_KEY), year, doc.getDatum(Document.VENUE_KEY),
                    doc.getDatum(Document.VEN_TYPE_KEY), doc.getDatum(Document.PAGES_KEY), vol,
                    num, doc.getDatum(Document.PUBLISHER_KEY), doc.getDatum(Document.PUBADDR_KEY),
                    doc.getDatum(Document.TECH_KEY), doc.getState(), doc.getVersionName(),
                    new Timestamp(crawlDate.getTime()),
                    finfo.getDatum(DocumentFileInfo.REP_ID_KEY),
                    finfo.getDatum(DocumentFileInfo.CONV_TRACE_KEY), doc.getDatum(Document.DOI_KEY) };
            return this.update(params);
        } // - UpdateDoc.run

    } // - class UpdateDoc

    private static final String DEF_UPDATE_DOC_SRC_QUERY = "update papers_versionShadow set title=?, abstract=?, "
            + "year=?, venue=?, venueType=?, pages=?, volume=?, number=?, "
            + "publisher=?, pubAddress=?, tech=?, citations=? where id=?";

    private class UpdateDocSrc
        extends SqlUpdate
    {

        public UpdateDocSrc(final DataSource dataSource)
        {
            this.setDataSource(dataSource);
            this.setSql(DEF_UPDATE_DOC_SRC_QUERY);
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.compile();
        } // - UpdateDocSrc.UpdateDocSrc

        public int run(final Document doc)
        {
            Object[] params = new Object[] { doc.getSource(Document.TITLE_KEY),
                    doc.getSource(Document.ABSTRACT_KEY), doc.getSource(Document.YEAR_KEY),
                    doc.getSource(Document.VENUE_KEY), doc.getSource(Document.VEN_TYPE_KEY),
                    doc.getSource(Document.PAGES_KEY), doc.getSource(Document.VOL_KEY),
                    doc.getSource(Document.NUM_KEY), doc.getSource(Document.PUBLISHER_KEY),
                    doc.getSource(Document.PUBADDR_KEY), doc.getSource(Document.TECH_KEY),
                    doc.getSource(Document.CITES_KEY), doc.getDatum(Document.DOI_KEY) };
            return this.update(params);
        } // - UpdateDocSrc.run

    } // - class UpdateDocSrc

    private static final String DEF_SET_PUBLIC_QUERY = "update papers set public=? where id=?";

    private class SetState
        extends SqlUpdate
    {

        public SetState(final DataSource dataSource)
        {
            this.setDataSource(dataSource);
            this.setSql(DEF_SET_PUBLIC_QUERY);
            this.declareParameter(new SqlParameter(Types.TINYINT));
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.compile();
        } // - SetPublic.SetPublic

        public int run(final Document doc, final int toState)
        {
            Object[] params = new Object[] { new Integer(doc.getState()),
                    doc.getDatum(Document.DOI_KEY) };
            return this.update(params);
        } // - SetPublic.run

    } // - class SetPublic

    private static final String DEF_SET_CLUSTER_QUERY = "update papers set cluster=? where id=?";

    private class SetCluster
        extends SqlUpdate
    {

        public SetCluster(final DataSource dataSource)
        {
            this.setDataSource(dataSource);
            this.setSql(DEF_SET_CLUSTER_QUERY);
            this.declareParameter(new SqlParameter(Types.BIGINT));
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.compile();
        } // - SetCluster.SetCluster

        public int run(final Document doc, final Long clusterID)
        {
            Object[] params = new Object[] { clusterID, doc.getDatum(Document.DOI_KEY) };
            return this.update(params);
        } // - SetCluster.run

    } // - class SetCluster

    private static final String DEF_SET_CITES_STMT = "update papers set ncites=? where id=?";

    private class SetNcites
        extends SqlUpdate
    {

        public SetNcites(final DataSource dataSource)
        {
            this.setDataSource(dataSource);
            this.setSql(DEF_SET_CITES_STMT);
            this.declareParameter(new SqlParameter(Types.INTEGER));
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.compile();
        } // - SetNcites.SetNcites

        public int run(final Document doc, final int ncites)
        {
            Object[] params = new Object[] { new Integer(ncites), doc.getDatum(Document.DOI_KEY) };
            return this.update(params);
        } // - SetNcites.run

    } // - class SetNCites

    private static final String DEF_COUNT_DOCUMENTS_QUERY = "select count(id) from papers";

    private class CountDocs
        extends MappingSqlQuery
    {

        public CountDocs(final DataSource dataSource)
        {
            this.setDataSource(dataSource);
            this.setSql(DEF_COUNT_DOCUMENTS_QUERY);
            this.compile();
        } // - CountDocs.CountDocs

        @Override
        public Integer mapRow(final ResultSet rs, final int rowNum)
            throws SQLException
        {
            return rs.getInt(1);
        } // - CountDocs.mapRow

        public Integer run()
        {
            List<Integer> list = this.execute();
            if (list.isEmpty())
                return null;
            else
                return list.get(0);
        } // - CountDocs.run

    } // - class CountDocs

    private static final String DEF_GET_DOIS_QUERY = "select id from papers where id>? order by id asc limit ?";

    private class GetDOIs
        extends MappingSqlQuery
    {

        public GetDOIs(final DataSource dataSource)
        {
            this.setDataSource(dataSource);
            this.setSql(DEF_GET_DOIS_QUERY);
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.declareParameter(new SqlParameter(Types.INTEGER));
            this.compile();
        } // - GetDOIs.GetDOIs

        @Override
        public String mapRow(final ResultSet rs, final int rowNum)
            throws SQLException
        {
            return rs.getString(1);
        } // - GetDOIs.mapRow

        public List<String> run(final String start, final int amount)
        {
            Object[] params = new Object[] { start, new Integer(amount) };
            return this.execute(params);
        } // - GetDOIs.run

    } // - class GetDOIs

    private static final String GET_ALL_DOIS_QUERY = "select id from papers order by id asc";

    /**
     * Encapsulates a query that returns all DOIs in the database
     *
     * @author Roland Kluge
     */
    private class GetAllDOIs
        extends MappingSqlQuery
    {

        public GetAllDOIs(final DataSource dataSource)
        {
            this.setDataSource(dataSource);
            this.setSql(GET_ALL_DOIS_QUERY);
            this.compile();
        }

        @Override
        public String mapRow(final ResultSet rs, final int rowNum)
            throws SQLException
        {
            return rs.getString(DAOConstants.PAPERS_ID);
        }

        public List<String> run()
        {
            return this.execute();
        }

    }

    /* start, end, prev, count */
    private static final String DEF_GET_SET_DOIS_QUERY = "select id, versionTime from papers where Date(versionTime) >= ? and "
            + "Date(versionTime) <= ? and id > ? and public = 1 order by id asc limit ?";

    private class GetSetDOIs
        extends MappingSqlQuery
    {

        public GetSetDOIs(final DataSource dataSource)
        {
            this.setDataSource(dataSource);
            this.setSql(DEF_GET_SET_DOIS_QUERY);
            this.declareParameter(new SqlParameter(Types.TIMESTAMP));
            this.declareParameter(new SqlParameter(Types.TIMESTAMP));
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.declareParameter(new SqlParameter(Types.INTEGER));
            this.compile();
        } // - GetSetDOIs.GetSetDOIs

        @Override
        public DOIInfo mapRow(final ResultSet rs, final int rowNum)
            throws SQLException
        {
            DOIInfo doi = new DOIInfo();

            doi.setDoi(rs.getString("id"));
            doi.setModifiedDate(rs.getTimestamp("versionTime"));
            return doi;
        } // - GetSetDOIs.mapRow

        public List<DOIInfo> run(final Date start, final Date end, final String prev,
                final int amount)
        {
            Object[] params = new Object[] { start, end, prev, new Integer(amount) };
            return this.execute(params);
        } // - GetSetDOIs.run

    } // - class GetSetDOIs

    /* start, end, prev */
    private static final String DEF_GET_SET_DOI_COUNT_QUERY = "select count(id) from papers where Date(versionTime) >= ? and "
            + "Date(versionTime) <= ? and id > ? and public = 1";

    private class GetSetDOICount
        extends MappingSqlQuery
    {

        public GetSetDOICount(final DataSource dataSource)
        {
            DocumentDAOImpl.this.setDataSource(dataSource);
            this.setSql(DEF_GET_SET_DOI_COUNT_QUERY);
            this.declareParameter(new SqlParameter(Types.TIMESTAMP));
            this.declareParameter(new SqlParameter(Types.TIMESTAMP));
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.compile();
        } // - GetSetDOICount

        @Override
        public Integer mapRow(final ResultSet rs, final int rowNum)
            throws SQLException
        {
            return rs.getInt(1);
        } // - GetSetDOICount.mapRow

        public Integer run(final Date start, final Date end, final String prev)
        {
            Object[] params = new Object[] { start, end, prev };
            List<Integer> rlist = this.execute(params);
            if (rlist != null) {
                if (rlist.isEmpty())
                    return null;
                else
                    return rlist.get(0);
            }
            else
                return null;
        } // - GetSetDOICount.run
    } // - class GetSetDOICount

    private static final String DEF_GET_CRAWLED_DOIS_BETWEEN_QUERY = "select id from papers where crawlDate > ? and crawlDate <= ? and "
            + "id > ? order by id asc limit ?";

    private class GetCrawledDOIs
        extends MappingSqlQuery
    {

        public GetCrawledDOIs(final DataSource dataSource)
        {
            this.setDataSource(dataSource);
            this.setSql(DEF_GET_CRAWLED_DOIS_BETWEEN_QUERY);
            this.declareParameter(new SqlParameter(Types.TIMESTAMP));
            this.declareParameter(new SqlParameter(Types.TIMESTAMP));
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.declareParameter(new SqlParameter(Types.INTEGER));
            this.compile();
        } // - GetSetDOIs.GetSetDOIs

        @Override
        public String mapRow(final ResultSet rs, final int rowNum)
            throws SQLException
        {
            return rs.getString("id");
        } // - GetCrawledDOIs.mapRow

        public List<String> run(final Date start, final Date end, final String lastID,
                final int amount)
        {
            Object[] params = new Object[] { start, end, lastID, new Integer(amount) };
            return this.execute(params);
        } // - GetCrawledDOIs.run

    } // - class GetCrawledDOIs

    private static final String DEF_GET_LATEST_DOCUMENTS_QUERY = "select id from papers where id < ? order by crawlDate desc, "
            + "id desc limit ?";

    private class GetLatestDocuments
        extends MappingSqlQuery
    {

        public GetLatestDocuments(final DataSource dataSource)
        {
            this.setDataSource(dataSource);
            this.setSql(DEF_GET_LATEST_DOCUMENTS_QUERY);
            this.declareParameter(new SqlParameter(Types.VARCHAR));
            this.declareParameter(new SqlParameter(Types.INTEGER));
            this.compile();
        } // - GetSetDOIs.GetSetDOIs

        @Override
        public String mapRow(final ResultSet rs, final int rowNum)
            throws SQLException
        {
            return rs.getString("id");
        } // - GetCrawledDOIs.mapRow

        public List<String> run(final String lastID, final int amount)
        {
            Object[] params = new Object[] { lastID, new Integer(amount) };
            return this.execute(params);
        } // - GetLatestDocuments.run

    } // - class GetLatestDocuments

} // - class DocumentDAOImpl
