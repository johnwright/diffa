/**
 * Copyright (C) 2010 LShift Ltd.
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

package net.lshift.diffa.util

import javax.servlet.http.{HttpServletRequestWrapper, HttpServletResponse, HttpServletRequest, HttpServlet}

/**
 * Simple utility for wrapping access to the default dispatcher, allowing static resources to be served
 * whilst still having a Spring dispatcher mapped as /.
 *
 * Based on solution provided at:
 *  http://stackoverflow.com/questions/132052/servlet-for-serving-static-content/837020#837020 
 */

class DefaultWrapperServlet extends HttpServlet {
  override def doGet(req:HttpServletRequest, resp:HttpServletResponse) = {
    val rd = getServletContext().getNamedDispatcher("default");

    val wrapped = new HttpServletRequestWrapper(req) {
      override def getServletPath =  ""
    }

    rd.forward(wrapped, resp)
  }
}
