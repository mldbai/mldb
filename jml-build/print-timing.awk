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
END {
    printf("[ %5.1fs %4.1fG %4.1fc ]", user+sys, rss*0.000001, cores);
}
