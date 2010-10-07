---
title: $resource.method$ $resource.nicePath$ | REST API Documentation
layout: default
---

<div id="menu" markdown="1">
$rest_menu()$
</div>

<div id="resources" markdown="1">
$resource.method$ $resource.nicePath$
=======================================================

<em>$resource.description$</em>

Entity Type
-----------
$resource.entityName$

URL
---
http://server:port/diffa-agent/rest/$resource.nicePath$

$if(resource.mandatoryParameters)$ 
Mandatory Parameters
--------------------
$resource.mandatoryParameters: { p |

### $p.name$

*$p.datatype$*

$p.description$
}$
$endif$

$if(resource.optionalParameters)$ 
Optional Parameters
-------------------
$resource.optionalParameters: { p |

### $p.name$

*$p.datatype$*

$p.description$
}$
$endif$

Requires Authentication
-----------------------
no $! TODO Make dynamic !$

Example
-------
``$resource.example$ ``

JSON Schema
-----------
``$resource.schema$ ``
</div>