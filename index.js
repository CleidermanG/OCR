var fs = require('fs');
const cron = require("node-cron");
const moment = require('moment');
const path = require('path');

const random = require('random');
const elasticsearch = require('elasticsearch');
const dir = "./json";
const dir2 = "./json2";
const esClient = new elasticsearch.Client({
    host: 'localhost:9200',
    log: 'error',
});

function save(dir, json) {
    for (var i = 0; i < json.length; i++) {
        json[i].total = json[i].total + random.int(0, 10);
    }
    return new Promise(resolve => {
        fs.writeFile(dir, JSON.stringify(json), function(err) {
            if (err) throw err;
            resolve('completado');
        });

    });
}


function saveCalculos(dir, solicitudope, solicitudes) {

    solicitudope[0].totalsolicitudes = 0;
    for (var i = 0; i < solicitudes.length; i++) {
        if (solicitudes[i].estado == 'Cerradas') {
            solicitudope[0].totalcerradas = solicitudes[i].total;
        }
        solicitudope[0].totalsolicitudes += solicitudes[i].total;
    }

    return new Promise(resolve => {
        fs.writeFile(dir, JSON.stringify(solicitudope), function(err) {
            if (err) throw err;
            resolve(solicitudope);
        });

    });
}

function cargar(dir) {
    return new Promise(resolve => {
        var dataRaw = fs.readFileSync(dir);
        var data = JSON.parse(dataRaw);
        resolve(data);
    });
}


cron.schedule("*/10 * * * * *", async function() {

    var solicitudes = await cargar('./json/solicitudes.json');
    save(`./json/solicitudes.json`, solicitudes);
    Delete('solicitudes', 'Logistica');
    bulkIndex('solicitudes', 'Logistica', solicitudes);

    var solicitudope = await cargar('./calculos/solicitudope.json');
    saveCalculos('./calculos/solicitudope.json', solicitudope, solicitudes);
    Delete('solicitudope', 'Logistica');
    bulkIndex('solicitudope', 'Logistica', solicitudope);

    var dataRaw = fs.readFileSync('./json/envios.json');
    const envios = JSON.parse(dataRaw);
    save(`./json/envios.json`, envios);
    Delete('envios', 'Logistica');
    bulkIndex('envios', 'Logistica', envios);

    dataRaw = fs.readFileSync('./json/devoluciones.json');
    const devoluciones = JSON.parse(dataRaw);
    save(`./json/devoluciones.json`, devoluciones);
    Delete('devoluciones', 'Logistica');
    bulkIndex('devoluciones', 'Logistica', devoluciones);

});



function bulkIndex(index, type, data) {

    let bulkBody = [];
    data.forEach(item => {
        bulkBody.push({
            index: {
                _index: index,
                _type: type,
                _id: item.id
            }
        });
        bulkBody.push(item);
    });

    esClient.bulk({ body: bulkBody })
        .then(response => {
            let errorCount = 0;
            response.items.forEach(item => {
                if (item.index && item.index.error) {
                    console.log(++errorCount, item.index.error);
                }
            });
            console.log(`Successfully indexed ${data.length - errorCount} out of ${data.length} items`);
        })
        .catch(console.err);
};

function Delete(index, type) {
    esClient.deleteByQuery({
        index: index,
        type: type,
        body: {
            query: {
                match_all: {}
            }
        }
    });
};