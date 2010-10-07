Contents
--------

$collections: { coll |
### $coll.className$$\n$
$coll.resources: { res |
$if(res.active)$
* $res.method$ $res.nicePath$$\n$$! currently selected !$
$else$
* [$res.method$ $res.nicePath$]($basePath$/$res.docPath$)$\n$
$endif$
}$
}$