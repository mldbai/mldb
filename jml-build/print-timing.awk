FNR == 1 {
    ++FNUM;
}
/User time/ {
    user=$4*1;
}
/System time/ {
    sys=$4*1;
}
/Maximum resident/ {
    rss=$6;
}
/Percent of CPU/ {
    cores=substr($7, 1, length($7-1))*0.01;
}
FNR == 1 && FNUM==2 {
    loc = $1
    tokens = $2
    codesize = $3
}
END {
    if (loc) {
        printf("[ %5.1fs %4.1fG %4.1fc ] [ %3.0fkl %4.1fM ]", user+sys, rss*0.000001, cores, loc / 1000, codesize / 1000000);
    }
    else {
        printf("[ %5.1fs %4.1fG %4.1fc ]", user+sys, rss*0.000001, cores);
    }   
}
 
