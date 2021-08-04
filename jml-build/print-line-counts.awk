FNR == 1 {
    loc = $1
    tokens = $2
    codesize = $3
}
END {
    printf("%3.0fkl %4.1fM", loc / 1000, codesize / 1000000);
}
 
