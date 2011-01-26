/**
 * Copyright (C) 2010-2011 LShift Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.lshift.diffa.docgen;

import java.util.List;

public class ResourceCollection {
    private final String name;
    private final String path;
    private final List<ResourceDescriptor> resources;

    public ResourceCollection(String name, String path, List<ResourceDescriptor> resources) {
        this.name = name;
        this.path = path;
        this.resources = resources;
    }

    public String getName() {
        return name;
    }

    public String getClassName() {
        return name.substring(1+name.lastIndexOf('.'));
    }

    public String getPath() {
        return path;
    }
  
    public List<ResourceDescriptor> getResources() {
        return resources;
    }

    /**
     * Sets active resource in the collection - convenience for StringTemplate.
     * @param rdToActivate resource to activate; null to deactivate all
     */
    public void setActiveResource(ResourceDescriptor rdToActivate) {
        for (ResourceDescriptor rd : resources)
            rd.setActive(rd.equals(rdToActivate));
    }

    public String getShortPath() {
        return (path.charAt(0) == '/') ? path.substring(1) : path; 
    }
}
