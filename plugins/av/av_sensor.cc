/** av_sensor.cc                                                   -*- C++ -*-

    Audio and video sensors.
*/

#include "av_plugin.h"
#include "mldb/core/sensor.h"

namespace Datacratic {
namespace MLDB {

struct CaptureAudioSensorConfig {
};

DECLARE_STRUCTURE_DESCRIPTION(CaptureAudioSensorConfig);
DEFINE_STRUCTURE_DESCRIPTION(CaptureAudioSensorConfig);

CaptureAudioSensorConfigDescription::
CaptureAudioSensorConfigDescription()
{
}


struct CaptureAudioSensor: public Sensor {

};

static RegisterSensorType<CaptureAudioSensor, CaptureAudioSensorConfig>
regAudioSensor(avPackage(),
               "av.capture.audio",
               "Audio capture sensor for microphones/webcams/USB audio sources",
               "CaptureAudioSensor.md.html");


} // namespace MLDB
} // namespace Datacratic

