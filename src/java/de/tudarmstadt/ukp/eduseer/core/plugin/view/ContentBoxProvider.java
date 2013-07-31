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

import java.util.List;

/**
 * A content box provider serves as the means by which content can be handed to the view layer.
 *
 * A content box is shown at the right of the document's summary site and typically contains short
 * metadata.
 *
 * @author Roland Kluge
 *
 */
public interface ContentBoxProvider
{
    /**
     * Returns the content boxes which are tailored to a given document
     *
     * @param doi the DOI of the document
     * @return the content boxes to be shown for this document
     */
    public List<ContentBox> getContentBoxes(final String doi);

}
