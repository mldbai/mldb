function assertEqual(expr, val, msg)
{
    if (expr == val)
        return;
    if (JSON.stringify(expr) == JSON.stringify(val))
        return;

    plugin.log("expected", val);
    plugin.log("received", expr);

    throw "Assertion failure: " + msg + ": " + JSON.stringify(expr)
        + " not equal to " + JSON.stringify(val);
}

var xls2csvTestUris = [
    "datetime.xlsx",
    "empty_row.xlsx",
    "escape.xlsx",
    "float.xlsx",
    "hyperlinks.xlsm",
    "hyperlinks_continous.xlsm",
    "junk-small.xlsx",
    "last-column-empty.xlsx",
    "namespace.xlsx",
    "sheets.xlsx",
    "skip_empty_lines.xlsx",
    "twolettercolumns.xlsx",
    "xlsx2csv-test-file.xlsx"
];

var expected = {
    "datetime.xlsx": [
        [ "_rowName", "A" ],
        [ "Sheet1:1", "2011-09-15T15:22:00Z" ]
    ],
    "empty_row.xlsx": [
        [
            "_rowName",
            "A",
            "B",
            "C",
            "D",
            "E",
            "F",
            "G",
            "H",
            "I",
            "J",
            "K",
            "L",
            "M"
        ],
        [ "Sheet1:1", "", "", "", "", "", "", "", "", "", "", "", "", "" ],
        [
            "Sheet1:2",
            "Date",
            "Agency",
            "Customer",
            "Campaign",
            "Publisher",
            "Format",
            "Inventory",
            "Impressions",
            "Clicks",
            "CTR (%)",
            "Price",
            "Price model",
            "Revenue"
        ],
        [
            "Sheet1:3",
            "At the moment no data for report",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        ]
    ],
    "escape.xlsx": [
        [ "_rowName", "E", "F", "A", "B" ],
        [ "Austin:1", "Hello\nWorld\t!", 0, null, null ],
        [ "Sheet2:1", null, null, 1, 2 ]
    ],
    "float.xlsx": [
        [ "_rowName", "A" ],
        [ "Лист1:2", 0.1030 ],
        [ "Лист1:3", 0.2760 ],
        [ "Лист1:4", 0.1030 ],
        [ "Лист1:5", 0.2760 ]
    ],
    // TODO: extract hyperlinks
    "hyperlinks.xlsm": [
        [ "_rowName", "A", "B", "C", "D", "E" ],
        [ "Sheet1:1", "google", "yahoo", "gmail", "reddit", "github" ]
    ],
    "hyperlinks_continous.xlsm": [
        [ "_rowName", "A", "B", "C", "D" ],
        [ "Sheet1:01", "google", "test", "yahoo", "reddit" ],
        [ "Sheet1:02", "google", "test", "yahoo", "reddit" ],
        [ "Sheet1:03", "google", "test", "yahoo", "reddit" ],
        [ "Sheet1:04", "google", "test", "yahoo", "reddit" ],
        [ "Sheet1:05", "google", "test", "yahoo", "reddit" ],
        [ "Sheet1:06", "google", "test", "yahoo", "reddit" ],
        [ "Sheet1:07", "google", "test", "yahoo", "reddit" ],
        [ "Sheet1:08", "google", "test", "yahoo", "reddit" ],
        [ "Sheet1:09", "google", "test", "yahoo", "reddit" ],
        [ "Sheet1:10", "reddit", "yahoo", "test", "google" ],
        [ "Sheet1:11", "reddit", "yahoo", "test", "google" ],
        [ "Sheet1:12", "reddit", "yahoo", "test", "google" ],
        [ "Sheet1:13", "reddit", "yahoo", "test", "google" ],
        [ "Sheet1:14", "reddit", "yahoo", "test", "google" ],
        [ "Sheet1:15", "reddit", "yahoo", "test", "google" ],
        [ "Sheet1:16", "reddit", "yahoo", "test", "google" ],
        [ "Sheet1:17", "reddit", "yahoo", "test", "google" ],
        [ "Sheet1:18", "reddit", "yahoo", "test", "google" ]
    ],
    "junk-small.xlsx": [
        [ "_rowName", "A", "B", "C", "D", "E", "F" ],
        [
            "Austin:1",
            "1940-03-29T00:00:00Z",
            "2008-07-25T00:00:00Z",
            "2008-07-25T00:00:00Z",
            "2009-04-08T00:00:00Z",
            "test",
            0
        ],
        [ "Sheet2:1", 1, 2, null, null, null, null ]
    ],
    "last-column-empty.xlsx": [
        [ "_rowName", "A", "B", "C" ],
        [ "Sheet1:1", "A", "B", "C" ],
        [ "Sheet1:2", "stuff", "more stuff", null ],
        [ "Sheet1:3", "things", "more things", "even more things" ],
        [ "Sheet1:4", "a", "b", null ],
        [ "Sheet1:5", "one", "two", null ],
        [ "Sheet1:6", 1, 2, 3 ]
    ],
    "namespace.xlsx": [
        [
            "_rowName",
            "A",
            "E",
            "P",
            "Q",
            "B",
            "C",
            "D",
            "F",
            "I",
            "L",
            "O",
            "G",
            "H",
            "J",
            "K",
            "M",
            "N"
        ],
        [
            "Data:1",
            "Case # (aka tissue code):",
            "SW101014-03",
            "+",
            "very light",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        ],
        [
            "Data:2",
            "Injection Site Location (PHAL/CTB):",
            "VISC Visceral Cortex (VISC) encroaching on the Gustatory Cortex (GU)",
            "++",
            "light",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        ],
        [
            "Data:3",
            "Injection Site Location (BDA/FG):",
            "Ssp Primary Somatosensory Cortex (SSp) Layers 4 and 5",
            "+++",
            "moderate",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        ],
        [
            "Data:4",
            null,
            null,
            "++++",
            "strong",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        ],
        [
            "Data:5",
            "Summary Notes:",
            "CTb and BDA did not work",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        ],
        [
            "Data:6",
            "Atlas level",
            "Anatomical Structure",
            null,
            null,
            "Data section  (File Name)",
            "Data section (LIMS)",
            "Anatomical Abbr",
            "PHAL",
            "CTB",
            "BDA",
            "FG",
            null,
            null,
            null,
            null,
            null,
            null
        ],
        [
            "Data:7",
            null,
            null,
            "Ipsi",
            "Notes",
            null,
            null,
            null,
            "Contra",
            "Contra",
            "Contra",
            "Contra",
            "Ipsi",
            "Notes",
            "Ipsi",
            "Notes",
            "Ipsi",
            "Notes"
        ]
    ],
    "sheets.xlsx": [
        [ "_rowName", "A", "B", "C", "D", "E", "F", "G" ],
        [
            "Вариант использования:01",
            "№",
            "Элемент",
            "Описание",
            "Результат шага (Выход)",
            "Ссылки",
            null,
            null
        ],
        [
            "Вариант использования:02",
            "1",
            "Номер",
            "Полный код (номер) сценария",
            null,
            null,
            null,
            null
        ],
        [
            "Вариант использования:03",
            "2",
            "Название",
            "Полное название сценария",
            null,
            null,
            null,
            null
        ],
        [
            "Вариант использования:04",
            "3",
            "Описание",
            "Краткое описание сути сценария",
            null,
            null,
            null,
            null
        ],
        [
            "Вариант использования:05",
            "4",
            "Тип",
            "Тип сценария / Уровень сценария - можно опустить из описания",
            null,
            null,
            null,
            null
        ],
        [
            "Вариант использования:06",
            "5",
            "Наследует",
            "Какой сценарий является родительским (базовым) для данного сценария",
            null,
            null,
            null,
            null
        ],
        [
            "Вариант использования:07",
            "6.1",
            "Актер ",
            "Кто основное действующее лицо. Если есть еще - добавляем",
            null,
            null,
            null,
            null
        ],
        [
            "Вариант использования:08",
            "6.2",
            "Система",
            "Кто основное действующее лицо с позиции системы (продукта). Если есть еще - добавляем",
            null,
            null,
            null,
            null
        ],
        [
            "Вариант использования:09",
            "7.1",
            "Цель",
            "Одна или несколько определенных целей для сценария",
            "Целевой показатель",
            null,
            null,
            null
        ],
        [
            "Вариант использования:10",
            "7.2",
            "Цель",
            null,
            null,
            null,
            null,
            null
        ],
        [
            "Вариант использования:11",
            "7.3",
            "Цель",
            null,
            null,
            null,
            null,
            null
        ],
        [
            "Вариант использования:12",
            "8.1",
            "Шаг",
            "Описание шага - кто что делает / с кем-чем взаимодействует",
            "Артефакт на выходе",
            null,
            null,
            null
        ],
        [
            "Вариант использования:13",
            "8.2",
            "Шаг",
            null,
            null,
            null,
            null,
            null
        ],
        [
            "Вариант использования:14",
            "8.х",
            "Шаг",
            null,
            null,
            null,
            null,
            null
        ],
        [
            "Вариант использования:15",
            "9",
            "Альт/Искл.",
            "название альтернативного потока / исключения",
            null,
            null,
            null,
            null
        ],
        [
            "Вариант использования:16",
            "9.1",
            "Шаг",
            null,
            null,
            null,
            null,
            null
        ],
        [
            "Вариант использования:17",
            "9.х",
            "Шаг",
            null,
            null,
            null,
            null,
            null
        ],
        [
            "Вариант использования:18",
            "10",
            "Альт/Искл.",
            "название альтернативного потока / исключения",
            null,
            null,
            null,
            null
        ],
        [
            "Вариант использования:19",
            "10.1",
            "Шаг",
            null,
            null,
            null,
            null,
            null
        ],
        [
            "Вариант использования:20",
            "10.х",
            "Шаг",
            null,
            null,
            null,
            null,
            null
        ],
        [
            "Реестр:1",
            "№",
            "URL",
            "Название",
            "Вер.",
            "Сост.",
            "Аналитик",
            "Заказчик"
        ],
        [
            "Реестр:2",
            1,
            "url",
            "<<Шаблон сценария>>",
            "1.0",
            "Подп.",
            "Фамилия ",
            "Фамилия"
        ],
        [ "Реестр:3", 2, null, null, null, null, null, null ],
        [ "Реестр:4", 3, null, null, null, null, null, null ],
        [ "Реестр:5", 4, null, null, null, null, null, null ],
        [ "Реестр:6", 5, null, null, null, null, null, null ]
    ],
    "skip_empty_lines.xlsx": [
        [ "_rowName", "A", "B", "C", "D", "E", "F", "G" ],
        [
            "Реестр:1",
            "№",
            "URL",
            "Название",
            "Вер.",
            "Сост.",
            "Аналитик",
            "Заказчик"
        ],
        [
            "Реестр:2",
            1,
            "url",
            "<<Шаблон сценария>>",
            "1.0",
            "Подп.",
            "Фамилия ",
            "Фамилия"
        ],
        [ "Реестр:3", null, null, null, null, null, null, null ],
        [ "Реестр:4", 3, null, null, null, null, null, null ],
        [ "Реестр:5", null, null, null, null, null, null, null ],
        [ "Реестр:6", null, null, null, null, null, null, null ]
    ],
    "twolettercolumns.xlsx": [
        [ "_rowName", "A", "B", "C", "D", "E", "F", "G", "H", "I", "Z", "AA", "AB" ],
        [ "Sheet1:1", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 ],
        [ "Sheet1:2", "a", "b", "c", "d", "e", "f", "g", null, null, "h", "I", "j" ]
    ],
    "xlsx2csv-test-file.xlsx": [
        [ "_rowName", "A", "B", "C" ],
        [ "Sheet1:01", "A", "B", "C" ],
        [ "Sheet1:02", null, "MSP", null ],
        [ "Sheet1:03", null, "MSP", null ],
        [ "Sheet1:04", null, "MSP", null ],
        [ "Sheet1:05", null, "MSP", null ],
        [ "Sheet1:06", null, "MSP", null ],
        [ "Sheet1:07", null, "MSP", null ],
        [ "Sheet1:08", null, "MSP", null ],
        [ "Sheet1:09", null, "MSP", null ],
        [ "Sheet1:10", null, "MSP", null ],
        [ "Sheet1:11", null, "MSP", null ],
        [ "Sheet1:12", null, "MSP", null ],
        [ "Sheet1:13", null, "MSP", null ],
        [ "Sheet1:14", null, "MSP", null ],
        [ "Sheet1:15", null, "MSP", null ],
        [ "Sheet1:16", null, "MSP", null ],
        [ "Sheet1:17", null, "MSP", null ],
        [ "Sheet1:18", null, "MSP", null ],
        [ "Sheet1:19", null, "MSP", null ],
        [ "Sheet1:20", null, "MSP", null ],
        [ "Sheet1:21", null, "MSP", null ],
        [ "Sheet1:22", null, "MSP", null ],
        [ "Sheet1:23", null, "MSP", null ],
        [ "Sheet1:24", null, "MSP", null ],
        [ "Sheet1:25", null, "MSP", null ],
        [ "Sheet1:26", null, "MSP", null ],
        [ "Sheet1:27", null, "MSP", null ],
        [ "Sheet1:28", null, "MSP", null ],
        [ "Sheet1:29", null, "MSP", null ],
        [ "Sheet1:30", null, "MSP", null ],
        [ "Sheet1:31", null, "MSP", null ],
        [ "Sheet1:32", null, "MSP", null ],
        [ "Sheet1:33", "blah", "PPS", null ],
        [ "Sheet1:34", "blah", "PPS", null ],
        [ "Sheet1:35", "blah", "PPS", null ],
        [ "Sheet1:36", "blah", "PPS", null ],
        [ "Sheet1:37", "blah", "PPS", null ],
        [ "Sheet1:38", "blah", "PPS", null ],
        [ "Sheet1:39", "blah", "PPS", null ],
        [ "Sheet1:40", "blah", "PPS", null ],
        [ "Sheet1:41", "blah", "PPS", null ],
        [ "Sheet1:42", "blah", "PPS", null ],
        [ "Sheet1:43", "blah", "PPS", null ],
        [ "Sheet1:44", "blah", "PPS", null ],
        [ "Sheet2:01", "A", "B", "C" ],
        [ "Sheet2:02", null, "MSP", null ],
        [ "Sheet2:03", null, "MSP", null ],
        [ "Sheet2:04", null, "MSP", null ],
        [ "Sheet2:05", null, "MSP", null ],
        [ "Sheet2:06", null, "MSP", null ],
        [ "Sheet2:07", null, "MSP", null ],
        [ "Sheet2:08", null, "MSP", null ],
        [ "Sheet2:09", null, "MSP", null ],
        [ "Sheet2:10", null, "MSP", null ],
        [ "Sheet2:11", null, "MSP", null ],
        [ "Sheet2:12", null, "MSP", null ],
        [ "Sheet2:13", null, "MSP", null ],
        [ "Sheet2:14", null, "MSP", null ],
        [ "Sheet2:15", null, "MSP", null ],
        [ "Sheet2:16", null, "MSP", null ],
        [ "Sheet2:17", null, "MSP", null ],
        [ "Sheet2:18", null, "MSP", null ],
        [ "Sheet2:19", null, "MSP", null ],
        [ "Sheet2:20", null, "MSP", null ],
        [ "Sheet2:21", null, "MSP", null ],
        [ "Sheet2:22", null, "MSP", null ],
        [ "Sheet2:23", null, "MSP", null ],
        [ "Sheet2:24", null, "MSP", null ],
        [ "Sheet2:25", null, "MSP", null ],
        [ "Sheet2:26", "blah", "PPS", null ],
        [ "Sheet2:27", "blah", "PPS", null ],
        [ "Sheet2:28", "blah", "PPS", null ],
        [ "Sheet2:29", "blah", "PPS", null ],
        [ "Sheet2:30", "blah", "PPS", null ],
        [ "Sheet2:31", "blah", "PPS", null ],
        [ "Sheet2:32", "blah", "PPS", null ],
        [ "Sheet2:33", "blah", "PPS", null ],
        [ "Sheet2:34", "blah", "PPS", null ],
        [ "Sheet2:35", "blah", "PPS", null ],
        [ "Sheet2:36", "blah", "PPS", null ],
        [ "Sheet2:37", "blah", "PPS", null ],
        [ "Sheet3:01", "A", "B", "C" ],
        [ "Sheet3:02", null, "MSP", null ],
        [ "Sheet3:03", null, "MSP", null ],
        [ "Sheet3:04", null, "MSP", null ],
        [ "Sheet3:05", null, "MSP", null ],
        [ "Sheet3:06", null, "MSP", null ],
        [ "Sheet3:07", null, "MSP", null ],
        [ "Sheet3:08", null, "MSP", null ],
        [ "Sheet3:09", null, "MSP", null ],
        [ "Sheet3:10", null, "MSP", null ],
        [ "Sheet3:11", null, "MSP", null ],
        [ "Sheet3:12", null, "MSP", null ],
        [ "Sheet3:13", "blah", "PPS", null ],
        [ "Sheet3:14", "blah", "PPS", null ],
        [ "Sheet3:15", "blah", "PPS", null ],
        [ "Sheet3:16", "blah", "PPS", null ],
        [ "Sheet3:17", "blah", "PPS", null ],
        [ "Sheet3:18", "blah", "PPS", null ],
        [ "Sheet3:19", "blah", "PPS", null ]
    ]
}

for (var i = 0;  i < xls2csvTestUris.length;  ++i) {
    mldb.log("loading", xls2csvTestUris[i]);

    var uri = "https://github.com/dilshod/xlsx2csv/raw/master/test/" + xls2csvTestUris[i];
    var config = {
        type: 'experimental.import.xlsx',
        params: {
            dataFileUrl: uri,
            output: {
                type: "sparse.mutable",
                id: "dataset" + i
            }
        }
    };

    var resp = mldb.put("/v1/procedures/x2c" + i, config);
    
    mldb.log("response is", resp);
    
    resp = mldb.put("/v1/procedures/x2c" + i + "/runs/1", {});
    
    mldb.log("run response is", resp);

    mldb.log("output of test", xls2csvTestUris[i], "is");

    var output = mldb.get("/v1/query", { q: "select * from dataset" + i + " order by rowName() limit 100",
                                     format: 'table'});

    mldb.log(output.json);

    var exp = expected[xls2csvTestUris[i]];

    assertEqual(output.json, exp, xls2csvTestUris[i]);
}

"success"
