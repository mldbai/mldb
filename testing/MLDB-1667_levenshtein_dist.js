
function doTestWords(a, b, score)
{
    var resp = mldb.query("select levenshtein_distance(" + mldb.sqlEscape(a) + ', ' + mldb.sqlEscape(b) + ") as dist");
}

doTestWords('patate', 'potato', 2)
doTestWords('', '', 0)
doTestWords('abcdef', 'poiuy', 6)
doTestWords('', 'poiuy', 5)
doTestWords('asdf', '', 4)
doTestWords('asdf', 'asffffffffff', 9)
doTestWords('brrasdfaseve', 'arras', 8)


var text = "All Good Things... comprises the 25th and 26th episodes of the "
    + "seventh season and the series finale of the syndicated American "
    + "science fiction television series Star Trek: The Next Generation"
    + ". It is the 177th and 178th episodes of the series overall. The "
    + "title is derived from the expression All good things must come "
    + "to an end, a phrase used by the character Q during the episode "
    + "itself. The finale was written as a valentine to the show''s fans"
    + ", and is now generally regarded as one of the series'' best "
    + "episodes. Hello";

var text2 = "All Good Things...  comprises the 25th and 26th episodes of the "
    + "seventh season and the series finale of the syndicated American "
    + "science fiction television series Star Trek: The Next Generation"
    + ". It is the 177th and 17 8th episodes of the series overall. The "
    + "title is derived from the expression All good things must come "
    + "to an end, a phrbse used by the character Q during the episoder "
    + "itself. The finale was written as a valentine to the show''s fans"
    + ", and is now generally regarded as one of the series'' best "
    + "episodes.";

for (var i = 0;  i < 500000;  ++i) {
    doTestWords(text, text2, 0)
}
        
"success"

