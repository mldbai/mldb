/** builtin_image_functions.cc
    Francois Maillet, 18 decembre 2016
    This file is part of MLDB. Copyright 2016 mldb.ai. All rights reserved.

*/

#include "mldb/sql/builtin_functions.h"
#include "mldb/ext/easyexif/exif.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/core/value_function.h"


using namespace std;



namespace MLDB {

// expression_value_description.cc
std::tuple<std::shared_ptr<ExpressionValueInfo>,
           ValueFunction::FromInput,
           ValueFunction::ToOutput>
toValueInfo(std::shared_ptr<const ValueDescription> desc);

namespace Builtins {

/*****************************************************************************/
/* IMAGE FUNCTIONS                                                           */
/*****************************************************************************/

struct ExifMetadata {
    ExpressionValue cameraMake;
    ExpressionValue cameraModel;
    ExpressionValue software;
    ExpressionValue bitsPerSample;
    ExpressionValue imageWidth;
    ExpressionValue imageHeight;
    ExpressionValue imageDescription;
    ExpressionValue imageOrientation;
    ExpressionValue imageCopyright;
    ExpressionValue imageDateTime;
    ExpressionValue originalDateTime;
    ExpressionValue digitizedDateTime;
    ExpressionValue subsecondTime;
    ExpressionValue exposureTime;
    ExpressionValue fStop;
    ExpressionValue isoSpeed;
    ExpressionValue subjectDistance;
    ExpressionValue exposureBias;
    ExpressionValue flashUsed;
    ExpressionValue meteringMode;
    ExpressionValue lensFocalLength;
    ExpressionValue focalLength35mm;
    ExpressionValue gpsLat;
    ExpressionValue gpsLon;
    ExpressionValue gpsAltitude;
    ExpressionValue gpsPrecision;
    ExpressionValue lensMinFocalLength;
    ExpressionValue lensMaxFocalLength;
    ExpressionValue lensFstopMin;
    ExpressionValue lensFstopMax;
    ExpressionValue lensMake;
    ExpressionValue lensModel;
    ExpressionValue focalPlaneXres;
    ExpressionValue focalPlaneYres;
};

DECLARE_STRUCTURE_DESCRIPTION(ExifMetadata);
DEFINE_STRUCTURE_DESCRIPTION(ExifMetadata);
ExifMetadataDescription::ExifMetadataDescription()
{
    addField("cameraMake", &ExifMetadata::cameraMake, "Camera Make");
    addField("cameraModel", &ExifMetadata::cameraModel, "Camera Model");
    addField("software", &ExifMetadata::software, "Software");
    addField("bitsPerSample", &ExifMetadata::bitsPerSample, "Bits Per Sample");
    addField("imageWidth", &ExifMetadata::imageWidth, "Image Width");
    addField("imageHeight", &ExifMetadata::imageHeight, "Image Height");
    addField("imageDescription", &ExifMetadata::imageDescription, "Image Description");
    addField("imageOrientation", &ExifMetadata::imageOrientation, "Image Orientation");
    addField("imageCopyright", &ExifMetadata::imageCopyright, "Image Copyright");
    addField("imageDateTime", &ExifMetadata::imageDateTime, "Image date/time");
    addField("originalDateTime", &ExifMetadata::originalDateTime, "Image original date/time");
    addField("digitizedDateTime", &ExifMetadata::digitizedDateTime, "Image digitized date/time");
    addField("subsecondTime", &ExifMetadata::subsecondTime, "Subsecond time");
    addField("exposureTime", &ExifMetadata::exposureTime, "Exposure time t: 1/t s");
    addField("fStop", &ExifMetadata::fStop, "F-stop x: f/x");
    addField("isoSpeed", &ExifMetadata::isoSpeed, "ISO Speed");
    addField("subjectDistance", &ExifMetadata::subjectDistance, "Subject Distance in meters");
    addField("exposureBias", &ExifMetadata::exposureBias, "Exposure bias in EV");
    addField("flashUsed", &ExifMetadata::flashUsed, "Flash used");
    addField("meteringMode", &ExifMetadata::meteringMode, "Metering mode");
    addField("lensFocalLength", &ExifMetadata::lensFocalLength, "Lens focal length");
    addField("focalLength35mm", &ExifMetadata::focalLength35mm, "35mm focal length");
    addField("gpsLat", &ExifMetadata::gpsLat, "GPS latitude");
    addField("gpsLon", &ExifMetadata::gpsLon, "GPS longitude");
    addField("gpsAltitude", &ExifMetadata::gpsAltitude, "GPS altitude in meters");
    addField("gpsPrecision", &ExifMetadata::gpsPrecision, "GPS Precision (DOP)");
    addField("lensMinFocalLength", &ExifMetadata::lensMinFocalLength, "Lens min focal length in mm");
    addField("lensMaxFocalLength", &ExifMetadata::lensMaxFocalLength, "Lens max focal length in mm");
    addField("lensFstopMin", &ExifMetadata::lensFstopMin, "Lens f-stop min x: f/x");
    addField("lensFstopMax", &ExifMetadata::lensFstopMax, "Lens f-stop max x: f/x");
    addField("lensMake", &ExifMetadata::lensMake, "Lens make");
    addField("lensModel", &ExifMetadata::lensModel, "Lens model");
    addField("focalPlaneXres", &ExifMetadata::focalPlaneXres, "Focal plane XRes");
    addField("focalPlaneYres", &ExifMetadata::focalPlaneYres, "Focal plane YRes");
}



BoundFunction extract_exif(const std::vector<BoundSqlExpression> & args)
{
    checkArgsSize(args.size(), 1, __FUNCTION__);

    std::shared_ptr<ExpressionValueInfo> outputInfo;
    std::function<ExpressionValue (const void * obj)> toOutput;
    std::tie(outputInfo, std::ignore, toOutput) =
        MLDB::toValueInfo(getDefaultDescriptionSharedT<ExifMetadata>());

    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                checkArgsSize(args.size(), 1);

                easyexif::EXIFInfo result;

                if(!args[0].isAtom())
                    throw MLDB::Exception("EXIF extraction requires that an atomic value "
                            "of type BLOB is passed to it. Use the fetcher function to get the image.");

                const CellValue & input = args[0].getAtom();

                if(!input.isBlob())
                    throw MLDB::Exception("EXIF extraction requires that an atomic value "
                            "of type BLOB is passed to it. Use the fetcher function to get the image.");

                const unsigned char * data = input.blobData();
                const size_t len = input.blobLength();

                int code = result.parseFrom(data, len);

                if (code) {
                    // TODO better errors
                    throw MLDB::Exception("Error parsing EXIF: code %d\n", code);
                }

                auto now = Date::now();

                // Dump EXIF information
                ExifMetadata exif;
                exif.cameraMake = ExpressionValue(std::move(result.Make), now);
                exif.cameraModel = ExpressionValue(std::move(result.Model), now);
                exif.software = ExpressionValue(std::move(result.Software), now);
                exif.bitsPerSample = ExpressionValue(result.BitsPerSample, now);
                exif.imageWidth = ExpressionValue(result.ImageWidth, now);
                exif.imageHeight = ExpressionValue(result.ImageHeight, now);
                exif.imageDescription = ExpressionValue(std::move(result.ImageDescription), now);
                exif.imageOrientation = ExpressionValue(result.Orientation, now);
                exif.imageCopyright = ExpressionValue(std::move(result.Copyright), now);
                exif.imageDateTime = ExpressionValue(std::move(result.DateTime), now);
                exif.originalDateTime = ExpressionValue(std::move(result.DateTimeOriginal), now);
                exif.digitizedDateTime = ExpressionValue(std::move(result.DateTimeDigitized), now);
                exif.subsecondTime = ExpressionValue(std::move(result.SubSecTimeOriginal), now);
                exif.exposureTime = ExpressionValue(result.ExposureTime, now);
                exif.fStop = ExpressionValue(result.FNumber, now);
                exif.isoSpeed = ExpressionValue(result.ISOSpeedRatings, now);
                exif.subjectDistance = ExpressionValue(result.SubjectDistance, now);
                exif.exposureBias = ExpressionValue(result.ExposureBiasValue, now);
                exif.flashUsed = ExpressionValue(result.Flash, now);
                exif.meteringMode = ExpressionValue(result.MeteringMode, now);
                exif.lensFocalLength = ExpressionValue(result.FocalLength, now);
                exif.focalLength35mm = ExpressionValue(result.FocalLengthIn35mm, now);
                exif.gpsLat = ExpressionValue(result.GeoLocation.Latitude, now);
                exif.gpsLon = ExpressionValue(result.GeoLocation.Longitude, now);
                exif.gpsAltitude = ExpressionValue(result.GeoLocation.Altitude, now);
                exif.gpsPrecision = ExpressionValue(result.GeoLocation.DOP, now);
                exif.lensMinFocalLength = ExpressionValue(result.LensInfo.FocalLengthMin, now);
                exif.lensMaxFocalLength = ExpressionValue(result.LensInfo.FocalLengthMax, now);
                exif.lensFstopMin = ExpressionValue(result.LensInfo.FStopMin, now);
                exif.lensFstopMax = ExpressionValue(result.LensInfo.FStopMax, now);
                exif.lensMake = ExpressionValue(std::move(result.LensInfo.Make), now);
                exif.lensModel = ExpressionValue(std::move(result.LensInfo.Model), now);
                exif.focalPlaneXres = ExpressionValue(result.LensInfo.FocalPlaneXResolution, now);
                exif.focalPlaneYres = ExpressionValue(result.LensInfo.FocalPlaneYResolution, now);

                return toOutput(&exif);
            },
            outputInfo
            };
}

static RegisterBuiltin registerExtractExif(extract_exif, "imageexif");



} // namespace Builtins
} // namespace MLDB

