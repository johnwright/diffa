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

import net.lshift.diffa.kernel.util.DocExamplesFactory;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.project.MavenProject;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @goal generate-docs
 * @requiresDependencyResolution compile
 */
public class DocGenMojo extends AbstractMojo
{
   /**
    * @parameter expression="${project}"
    * @required
    * @readonly
    */
    private MavenProject project;

    /**
     * @parameter
     * @required
     */
    private String templateDir;

    /**
     * @parameter
     * @required
     */
    private String[] resources;

    /**
     * @parameter
     */
    private String targetDir;

    /**
     * @parameter expression=false
     */
    private Boolean consoleOutput;

    /**
     * @parameter
     * @required
     */
    private String examplesFactoryClass;

    public void execute() throws MojoExecutionException
    {
        try {
            ClassLoader projectClassLoader = getProjectClassLoader();

            DocExamplesFactory examplesFactory = (DocExamplesFactory) Class.forName(examplesFactoryClass).newInstance();

            // IntelliJ doesn't like this but it's fine, trust me
            Map<Class<?>, Object> examples = examplesFactory.getExamples();

            RestGen gen = new RestGen(templateDir);
            for (String clazz : resources)
                gen.addResources(projectClassLoader.loadClass(clazz), examples);

            File targetDirectory = new File(targetDir);
            if (targetDirectory.exists())
                getLog().warn("Target directory exists.");

            for (Map.Entry<String, String> resEntry : gen.getFiles().entrySet())
                writeOut(resEntry.getKey(), resEntry.getValue());
        }
        catch (MojoExecutionException e) {
            throw e;
        }
        catch (Exception e) {
            throw new MojoExecutionException("Exception during diffa doc generation.", e);
        }
        // TODO assert something here
    }

    protected String getFilePathForResource(ResourceDescriptor resource) {
        return resource.getMethod().toLowerCase() + '/' + resource.getPath();
    }

    protected void writeOut(String fileName, String content) throws IOException {
        // File output
        if (targetDir != null && !targetDir.isEmpty()) {
            File outFile = new File(targetDir, fileName);
            File outParent = outFile.getParentFile();

            if (outParent.mkdirs())
                getLog().debug("Created target directory: " + outParent.getCanonicalPath());

            getLog().info("Writing " + outFile.getCanonicalPath());
            FileWriter out = new FileWriter(outFile);
            out.write(content);
            out.close();
        }

        // Console output
        if (consoleOutput) {
            System.out.println("Doc for page: " + fileName + "\n=====\n");
            System.out.println(content);
        }
    }

    protected ClassLoader getProjectClassLoader()  throws Exception {
        List compileClasspathElements = project.getCompileClasspathElements();
        List<URL> compileUrls = new ArrayList<URL>();
      
        for (Object element : compileClasspathElements)
            compileUrls.add(new File((String) element).toURI().toURL());

        return new URLClassLoader(compileUrls.toArray(new URL[compileUrls.size()]),
                Thread.currentThread().getContextClassLoader());
  }


}
