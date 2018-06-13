/** operation_test.cc                                              -*- C++ -*-
    Jeremy Barnes, 4 May 2018
    Copyright (c) 2018 Element AI Inc.  All rights reserved.

    Test of operations functionality.
*/

#include "mldb/block/operation.h"

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>

using namespace std;
using namespace MLDB;


BOOST_AUTO_TEST_CASE( test_operation )
{
#if 0
    std::string fileListName = "/mnt/datasets/public/imagenet/raw/train/files.txt";
    Parameter param("fileListName", fileListName);

    ReadBlockOperation readBlock;

    Block block = readBlock(param);
    
    SplitLinesOperation splitLines;
    
    BlockList synsets = splitLines(block);
#endif
    std::string filename = "/mnt/datasets/public/imagenet/raw/train/n02085782/n02085782_7499.JPEG";

    OperationContext context;
    context.immediateModeOnHost();
    
    Scalar var("filename", filename);

    ReadBlockOperation readBlock("readBlock");

    Block block = readBlock(var);

    cerr << "block = " << block << endl;
    
    DecodeJpegOperation decodeJpeg("decodeJpeg");

    Image image = decodeJpeg(block);

    Scalar width("width", image.width());
    Scalar height("height", image.height());

    cerr << "width = " << width << " height = " << height << endl;

    context.returnRow("dimensions", {width, height});

    cerr << "result " << context.getResult() << endl;
}
