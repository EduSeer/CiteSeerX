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
package de.tudarmstadt.ukp.eduseer.core.plugin.view;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * This content box provider renders its content as a comma-separated list.
 *
 * Each document gets assigned one content box. The items to be displayed are queried via an
 * abstract method (template pattern).
 *
 * @author Roland Kluge
 *
 */
public abstract class CompactListContentBoxProvider
    implements ContentBoxProvider
{

    private static final String DEFAULT_SEPARATOR = ",";

    private String separator;

    public CompactListContentBoxProvider()
    {
        this.setSeparator(DEFAULT_SEPARATOR);
    }

    /**
     * Returns the separator which separates items in the list
     *
     * @return the separator
     */
    public String getSeparator()
    {
        return this.separator;
    }

    /**
     * Sets the separator which separates items in the list
     *
     * @param separator
     *            the separator to be used. May not be null.
     */
    public void setSeparator(final String separator)
    {
        if (null == separator)
            throw new IllegalArgumentException("Separator must be non-null");

        this.separator = separator;
    }

    /**
     * Returns a singleton list with the content box.
     *
     * @param doi
     *            the DOI of the document. This parameter is ignored.
     *
     * @return the content box as a singleton list
     */
    @Override
    public List<ContentBox> getContentBoxes(final String doi)
    {
        final List<ContentBox> result = new ArrayList<ContentBox>();

        if (this.hasContentBox(doi)) {
            final String heading = this.getHeading(doi);
            final List<String> items = this.getItems(doi);

            final StringBuilder sb = new StringBuilder();
            for (final Iterator<String> iter = items.iterator(); iter.hasNext();) {
                final String item = iter.next();
                sb.append(item);
                if (iter.hasNext()) {
                    sb.append(this.separator);
                    sb.append(' ');
                }
            }

            final ContentBox contentBox = new ContentBox(heading, sb.toString());
            result.add(contentBox);
        }

        return result;
    }

    /**
     * Returns whether a content box shall be generated for a document.
     *
     * @param doi
     *            the doi of the document
     *
     * @return true a content box shall be created
     */
    protected abstract boolean hasContentBox(final String doi);

    /**
     * Returns the items which shall be rendered as comma-separated list.
     *
     * @param doi
     *            the DOI of the document
     *
     * @return list of items
     */
    protected abstract List<String> getItems(final String doi);

    /**
     * Returns the heading of the content box for a document.
     *
     * @param doi
     *            the DOI of the document
     *
     * @return the heading for the content box. Guaranteed to be non-null
     */
    protected abstract String getHeading(final String doi);

}
