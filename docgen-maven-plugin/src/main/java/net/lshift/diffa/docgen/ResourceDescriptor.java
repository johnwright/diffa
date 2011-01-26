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

import org.codehaus.jackson.schema.JsonSchema;

import java.util.ArrayList;
import java.util.List;

public class ResourceDescriptor {
    private String method = "";
    private String path = "";
    private String description = "";
    private List<MandatoryParameter> mandatoryParameters = new ArrayList<MandatoryParameter>();
    private List<OptionalParameter> optionalParameters = new ArrayList<OptionalParameter>();
    private String entityName = "";
    private JsonSchema schema;
    private String example;
    private boolean active; // is this the currently looked at element?; convenience for StringTemplate
    private String collectionPath;

    public ResourceDescriptor() {}

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public List<MandatoryParameter> getMandatoryParameters() {
        return mandatoryParameters;
    }

    public void setMandatoryParameters(List<MandatoryParameter> mandatoryParameters) {
        this.mandatoryParameters = mandatoryParameters;
    }

    public List<OptionalParameter> getOptionalParameters() {
        return optionalParameters;
    }

    public void setOptionalParameters(List<OptionalParameter> optionalParameters) {
        this.optionalParameters = optionalParameters;
    }

    public String getEntityName() {
        return entityName;
    }

    public void setEntityName(String entityName) {
        this.entityName = entityName;
    }

    public JsonSchema getSchema() {
        return schema;
    }

    public void setSchema(JsonSchema schema) {
        this.schema = schema;
    }

    public String getExample() {
        return example;
    }

    public void setExample(String example) {
        this.example = example;
    }

    /**
     * Returns the REST path decorated with colons in parameters.
     * @return
     */
    public String getNicePath() {
        // Replace {param} with :param
        String nicePath = path.replaceAll("\\{([a-zA-z0-9_-]*)\\}", ":$1");
        return getCleanCollectionPath() + nicePath;
    }

    /**
     * Returns the actual file path of a doc file.
     * @return
     */
    public String getDocPath() {
        // Replace {param} with p_param
        return getCleanCollectionPath() + '/' + method.toLowerCase() + path.replaceAll("\\{([a-zA-z0-9_-]*)\\}", "p_$1");
    }

    private String getCleanCollectionPath() {
        return collectionPath.startsWith("/") ? collectionPath.substring(1) : collectionPath;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public String getCollectionPath() {
        return collectionPath;
    }

    public void setCollectionPath(String collectionPath) {
        this.collectionPath = collectionPath;
    }
}
