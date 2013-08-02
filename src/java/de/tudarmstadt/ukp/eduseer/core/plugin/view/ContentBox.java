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

/**
 * This class is an abstract representation of the content boxes shown at the right on a
 * document's summary site.
 *
 * @author Roland Kluge
 *
 */
public class ContentBox
{

    private static final String DEFAULT_HEADING = "";
    private static final String DEFAULT_CONTENT = "";
    private String heading;
    private String content;

    /**
     * Creates a content box with empty heading and content.
     */
    public ContentBox()
    {
        this(DEFAULT_HEADING, DEFAULT_CONTENT);
    }

    /**
     * Creates a content box with given heading and content.
     *
     * @param heading
     *            the heading
     * @param content
     *            the content
     */
    public ContentBox(final String heading, final String content)
    {
        this.setHeading(heading);
        this.setContent(content);
    }

    /**
     * Returns the heading of the content box.
     *
     * @return the heading. Guaranteed to be non-null.
     */
    public String getHeading()
    {
        return this.heading;
    }

    /**
     * Sets the heading of this content box.
     *
     * @param heading the heading to be set. May not be null.
     */
    public void setHeading(final String heading)
    {
        if (null == heading)
            throw new IllegalArgumentException("Null heading not allowed!");

        this.heading = heading;
    }

    /**
     * Returns the content of the content box.
     *
     * @return the content. Guaranteed to be non-null.
     */
    public String getContent()
    {
        return this.content;
    }

    /**
     * Sets the content of this content box.
     *
     * @param content the content to be set. May not be null.
     */
    public void setContent(final String content)
    {
        if (null == content)
            throw new IllegalArgumentException("Null content not allowed!");

        this.content = content;
    }

}
