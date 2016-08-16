Sugar = require('sugar');
Sugar.extend();

var r = require('rethinkdb');
var config = require(__dirname + '/database.js')
var fs = require('fs'),
    JSONStream = require('JSONStream'),
    es = require('event-stream');

r.connect(config.rethinkdb, function (err, conn) {
    if (err) throw err
    var _rdbConn = conn;
    startParsing(_rdbConn, r)

})

var getStream = function () {
    // var jsonData = '../../zcta5.json',
        var jsonData = __dirname +'/test.json',
        stream = fs.createReadStream(jsonData, { encoding: 'utf8' }),
        parser = JSONStream.parse([true, { emitKey: true }]);
    return stream.pipe(parser);
}

function startParsing(_rdbConn) {
    getStream().on('data', function (data) {

        var latitude = []
        var longitude = []
        console.log(data.value.properties.ZCTA5CE10)
        // console.log(data.value.geometry.type)
        // console.log(data.value.geometry.coordinates)
        if (data.value.geometry.type === 'Polygon') {
            data.value.geometry.coordinates.forEach(function (item) {
                item.forEach(function (n) {
                    latitude.push(n[0])
                    longitude.push(n[1])
                })
            })
        }
        else {
            data.value.geometry.coordinates.forEach(function (element) {
                element.forEach(function (item) {
                    item.forEach(function (n) {
                        latitude.push(n[0])
                        longitude.push(n[1])
                    })
                })
            })

        }


        var polygon = ''
        var zip_code = data.value.properties.ZCTA5CE10
        polygon = latitude.max() + ' ' + longitude.min() + ',' + 
                  latitude.max() + ' ' + longitude.max() + ',' + 
                  latitude.min() + ' ' + longitude.max() + ',' + 
                  latitude.min() + ' ' + longitude.min() + ',' + 
                  latitude.max() + ' ' + longitude.min()
        
        try {
            var result = r.table('zip_code_polygon').insert({ 'id': zip_code, 'polygon': polygon }, { returnChanges: true }).run(_rdbConn);
        }
        catch (e) {
            console.log(e)
        }
    })

    getStream().on('end', function () {
        console.log('end');
    });

}


    // http://terminal2.expedia.com/x/geo/features?within=WKT:POLYGON((-67.786033 46.508813, -67.786033 46.599908, -67.908946 46.599908, -67.908946 46.508813, -67.786033 46.508813))&apikey=0b4WLhLx3EPmSPiE5bLyXieIsAL2szm3