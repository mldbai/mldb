#
# MLDB-2097_exif.py
# mldb.ai, 2016
# this file is part of mldb. copyright 2016 mldb.ai. all rights reserved.
#
import datetime, os

mldb = mldb_wrapper.wrap(mldb) # noqa

class Mldb2097Test(MldbUnitTest):  # noqa

    @classmethod
    def setUpClass(self):
        pass

    def test_all(self):

        num_images = 0
        for f in os.listdir("mldb/ext/easyexif/test-images"):
            if not f.endswith(".jpg"): continue
            mldb.log(">>> File: " + f)
            rez = mldb.query("""
                SELECT imageexif(fetcher('file://mldb/ext/easyexif/test-images/%s')[content]) as *
            """ % f)

            answers_lst = [x.strip().split(":", 1) for x in open('mldb/ext/easyexif/test-images/%s.expected' % f).readlines()]
            answers ={key.strip(): value.strip() for (key, value) in answers_lst}

            mldb.log(answers)

            num_images += 1
            for col_id, col_name in enumerate(rez[0]):
                if col_name == "_rowName": continue
                mldb.log(col_name)

                if col_name == "bitsPerSample":
                    self.assertEqual(int(answers["Bits per sample"]), rez[1][col_id])
                elif col_name == "cameraMake":
                    self.assertEqual(answers["Camera make"], rez[1][col_id])
                elif col_name == "cameraModel":
                    self.assertEqual(answers["Camera model"], rez[1][col_id])
                elif col_name == "lensMake":
                    self.assertEqual(answers["Lens make"], rez[1][col_id])
                elif col_name == "lensModel":
                    self.assertEqual(answers["Lens model"], rez[1][col_id])
                elif col_name == "imageCopyright":
                    self.assertEqual(answers["Image copyright"], rez[1][col_id])
                elif col_name == "imageDescription":
                    if rez[1][col_id] is None:
                        self.assertEqual(answers["Image description"].strip(), '')
                    else:
                        self.assertEqual(answers["Image description"], rez[1][col_id].strip())
                elif col_name == "software":
                    self.assertEqual(answers["Software"], rez[1][col_id])
                elif col_name == "imageHeight":
                    self.assertEqual(int(answers["Image height"]), rez[1][col_id])
                elif col_name == "imageWidth":
                    self.assertEqual(int(answers["Image width"]), rez[1][col_id])
                elif col_name == "imageOrientation":
                    self.assertEqual(int(answers["Image orientation"]), rez[1][col_id])
                elif col_name == "isoSpeed":
                    self.assertEqual(int(answers["ISO speed"]), rez[1][col_id])
                elif col_name == "lensFocalLength":
                    self.assertEqual(float(answers["Lens focal length"].split(" ")[0]), rez[1][col_id])
                elif col_name == "lensFstopMax":
                    self.assertEqual(float(answers["Lens f-stop max"].split("/")[1]), rez[1][col_id])
                elif col_name == "lensFstopMin":
                    self.assertEqual(float(answers["Lens f-stop min"].split("/")[1]), rez[1][col_id])
                elif col_name == "meteringMode":
                    self.assertEqual(float(answers["Metering mode"]), rez[1][col_id])
                elif col_name == "lensMinFocalLength":
                    #    "Lens min focal length": "0.000000 mm",
                    self.assertEqual(float(answers["Lens min focal length"].split(" ")[0]), rez[1][col_id])
                elif col_name == "lensMaxFocalLength":
                    self.assertEqual(float(answers["Lens max focal length"].split(" ")[0]), rez[1][col_id])
                elif col_name == "subjectDistance":
                    self.assertEqual(float(answers["Subject distance"].split(" ")[0]), rez[1][col_id])
                elif col_name == "subsecondTime":
                    self.assertEqual(answers["Subsecond time"], rez[1][col_id])
                elif col_name == "digitizedDateTime":
                    self.assertEqual(answers["Digitize date/time"], rez[1][col_id])
                elif col_name == "originalDateTime":
                    self.assertEqual(answers["Original date/time"], rez[1][col_id])
                elif col_name == "imageDateTime":
                    self.assertEqual(answers["Image date/time"], rez[1][col_id])
                elif col_name == "exposureBias":
                    self.assertEqual(float(answers["Exposure bias"].split(" ")[0]), rez[1][col_id])
                elif col_name == "exposureTime":
                    # "Exposure time": "1/640 s"
                    val = 1 / rez[1][col_id] if rez[1][col_id] > 0 else 0
                    self.assertEqual(float(answers["Exposure time"].split(" ")[0].split("/")[1]), val)
                elif col_name == "fStop":
                    #    "F-stop": "f/4.5",
                    self.assertEqual(answers["F-stop"], "f/%0.1f" % rez[1][col_id])
                elif col_name == "flashUsed":
                    #    "Flash used?": "1",
                    self.assertEqual(int(answers["Flash used?"]), rez[1][col_id])
                elif col_name == "focalLength35mm":
                    self.assertEqual(float(answers["35mm focal length"].split(" ")[0]), rez[1][col_id])
                elif col_name == "focalPlaneXres":
                    self.assertAlmostEqual(float(answers["Focal plane XRes"]), rez[1][col_id], places=5)
                elif col_name == "focalPlaneYres":
                    self.assertAlmostEqual(float(answers["Focal plane YRes"]), rez[1][col_id], places=5)
                elif col_name == "gpsAltitude":
                    #    "GPS Altitude": "0.000000 m",
                    self.assertAlmostEqual(float(answers["GPS Altitude"].split(" ")[0]), rez[1][col_id], places=5)
                elif col_name == "gpsLat":
                    #     "GPS Latitude": "0.000000 deg (0.000000 deg, 0.000000 min, 0.000000 sec ?)",
                    self.assertAlmostEqual(float(answers["GPS Latitude"].split(" ")[0]), rez[1][col_id], places=5)
                elif col_name == "gpsLon":
                    self.assertAlmostEqual(float(answers["GPS Longitude"].split(" ")[0]), rez[1][col_id], places=5)
                elif col_name == "gpsPrecision":
                    self.assertEqual(float(answers["GPS Precision (DOP)"]), rez[1][col_id])

                else:
                    raise Exception("unhandled key: " + col_name)

        self.assertEqual(num_images, 12)

if __name__ == '__main__':
    mldb.run_tests()
