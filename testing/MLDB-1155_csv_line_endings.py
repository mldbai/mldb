#
# MLDB-1155_csv_line_endings.py
# mldb.ai inc, 2015
# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
#
import gzip
import unittest
import os

if False:
    mldb_wrapper = None
mldb = mldb_wrapper.wrap(mldb) # noqa


class CsvLineEndingsGeneric(object):

    def store_load_into_file(self, filename, lines):
        filename += self.__class__.data['ext']
        with self.__class__.data['open_fct'](filename, 'wb') as f:
            for line in lines:
                f.write(line)

        csv_conf = {
            "type": "import.text",
            "params": {
                'dataFileUrl' : "file://" + filename,
                "outputDataset": {
                    "id": "x",
                },
                "runOnCreation" : True,
            }
        }
        mldb.put("/v1/procedures/csv_proc", csv_conf) 

        ds_result = mldb.get("/v1/datasets/x")

        q_result = mldb.query("select * from x")
        return ds_result, q_result

    def test_missing_last_column_test(self):
        # CSV importation should not complain about number of values when
        # missing last column
        ds_result, q_result = self.store_load_into_file(
            "tmp/csv_newline_missing_last_column",
            [
                "a,b\n",
                "1.0,\n", # NOTE missing value in last position
                "1.0,1.0\n",
                "1.0,\"hello\"\n"
            ]
        )
        self.assertEqual(ds_result.json()["status"]["rowCount"], 3)
        result = mldb.query(
            "select * from x order by cast (rowName() as number)")
        self.assertEqual(result[3][2], "hello")

    def test_missing_values_dos_line_endings(self):
        # CSV importation should not complain about number of values when
        # missing last column with DOS line endings
        ds_result, q_result = self.store_load_into_file(
            "tmp/csv_newline_missing_last_column_dos",
            [
                "a,b\r\n",
                "1.0,\r\n", # NOTE missing value in last position
                "1.0,1.0\r\n",
                "1.0,\"hello\"\r\n"
            ]
        )
        self.assertEqual(ds_result.json()["status"]["rowCount"], 3)

        result = mldb.query(
            "select * from x order by cast (rowName() as number)")
        self.assertEqual(result[3][2], "hello")

    def test_import_read_float_values_at_end_of_line(self):
        # CSV importation should correctly read float values at the ends of
        # lines
        ds_result, q_result = self.store_load_into_file(
            "tmp/csv_newline_float_last",
            [
                "a,b\n",
                "1.0,1.0\n"
            ]
        )
        self.assertEqual(ds_result.json()["status"]["rowCount"], 1)
        self.assertEqual(q_result[1][1], 1)
        self.assertEqual(q_result[1][2], 1)

    def test_import_read_float_values_end_of_line_with_dos(self):
        # CSV importation should correctly read float values at the ends of
        # lines with DOS
        ds_result, q_result = self.store_load_into_file(
            "tmp/csv_newline_float_last_dos",
            [
                "a,b\r\n",
                "1.0,1.0\r\n"
            ]
        )
        self.assertEqual(ds_result.json()["status"]["rowCount"], 1)
        self.assertEqual(q_result[1][1], 1)
        self.assertEqual(q_result[1][2], 1)

    def test_import_read_quoted_strings_at_end_of_lines(self):
        # CSV importation should correctly read quoted string values at the
        # ends of lines
        ds_result, q_result = self.store_load_into_file(
            "tmp/csv_newline_qstring_last",
            [
                "a,b\n",
                "1.0,\"hello\"\n"
            ]
        )
        self.assertEqual(ds_result.json()["status"]["rowCount"], 1)
        self.assertEqual(q_result[1][1], 1)
        self.assertEqual(q_result[1][2], "hello")

    def test_import_string_val_end_of_line(self):
        # CSV importation should correctly read string values at end of lines
        ds_result, q_result = self.store_load_into_file(
            "tmp/csv_newline_string_last",
            [
                "a,b\n",
                "1.0,\"hello\"\n"
            ]
        )
        self.assertEqual(ds_result.json()["status"]["rowCount"], 1)
        self.assertEqual(q_result[1][1], 1)
        self.assertEqual(q_result[1][2], "hello")

    def test_no_bad_seek_fail_on_missing_trailing_new_line(self):
        # CSV importation should not fail with "bad seek" if missing trailing
        # newline
        ds_result, q_result = self.store_load_into_file(
            "tmp/csv_newline_missing_last",
            [
                "a,b\n",
                "1.0,1.0" # NOTE no trailing newline on the last line
            ]
        )
        self.assertEqual(ds_result.json()["status"]["rowCount"], 1)
        self.assertEqual(q_result[1][1], 1)
        self.assertEqual(q_result[1][2], 1)

    def test_no_bad_seek_fail_on_missing_trailing_new_line_dos(self):
        # CSV importation should not fail with "bad seek" if missing trailing
        # newline with DOS line endings
        ds_result, q_result = self.store_load_into_file(
            "tmp/csv_newline_missing_last_dos",
            [
                "a,b\r\n",
                "1.0,1.0" # NOTE no trailing newline on the last line
            ]
        )
        self.assertEqual(ds_result.json()["status"]["rowCount"], 1)
        self.assertEqual(q_result[1][1], 1)
        self.assertEqual(q_result[1][2], 1)

    def test_import_accepts_blank_trailing_line(self):
        # CSV importation should accept a blank trailing line
        ds_result, q_result = self.store_load_into_file(
            "tmp/csv_newline_trailing_newlines",
            [
                "a,b\n",
                "1.0,1.0\n",
                "\n"  # NOTE: blank line at end
            ]
        )
        self.assertEqual(ds_result.json()["status"]["rowCount"], 1)
        self.assertEqual(q_result[1][1], 1)
        self.assertEqual(q_result[1][2], 1)

    def test_import_accept_blank_trailing_line_dos(self):
        # CSV importation should accept a blank trailing line with DOS
        ds_result, q_result = self.store_load_into_file(
            "tmp/csv_newline_trailing_newlines_dos",
            [
                "a,b\r\n",
                "1.0,1.0\r\n",
                "\r\n" # NOTE: blank line at end
            ]
        )
        self.assertEqual(ds_result.json()["status"]["rowCount"], 1)
        self.assertEqual(q_result[1][1], 1)
        self.assertEqual(q_result[1][2], 1)

    def test_empty_csv(self):
        # importing an empty file leads to an empty dataset
        path = "tmp/csv_empty.csv"
        with open(path, 'wb'):
            os.utime(path, None)

        csv_conf = {
            "type": "import.text",
            "params": {
                'dataFileUrl' : "file://" + path,
                "outputDataset": {
                    "id": "empty_csv",
                },
                "runOnCreation" : True,
            }
        }
        mldb.put("/v1/procedures/csv_proc", csv_conf) 

        result = mldb.get("/v1/query", q="SELECT count(*) FROM empty_csv")
        mldb.log(result.text)
        self.assertEqual(result.json()[0]["columns"][0][1], 0,
                         "expected row count of empty dataset to be 0")

        with self.assertRaises(mldb_wrapper.ResponseException) as exc:
            error_conf = {
                "type": "import.text",
                "params": {
                    'dataFileUrl' : "file://this/path/does/not/exist.csv",
                    "outputDataset": {
                        "id": "does_not_exist_csv",
                    },
                    "runOnCreation" : True,
                }
            }
            mldb.put("/v1/procedures/error_proc", error_conf) 

        result = exc.exception.response
        mldb.log(result.text)

        self.assertTrue("No such file or directory" in result.json()["details"]["runError"]["error"],
                        "did not get the expected message MLDB-1299")


class CsvLineEndingsCsv(CsvLineEndingsGeneric, unittest.TestCase):
    # held in a dict to counter unbount method issues
    data = {'open_fct' : open, 'ext' : '.csv'}


class CsvLineEndingsCsvGz(CsvLineEndingsGeneric, unittest.TestCase):
    data = {'open_fct' : gzip.open, 'ext' : '.csv.gz'}

if __name__ == '__main__':
    mldb.run_tests()
