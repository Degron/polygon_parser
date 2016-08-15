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
    var jsonData = '../../zcta5.json',
        stream = fs.createReadStream(jsonData, { encoding: 'utf8' }),
        parser = JSONStream.parse([true, {emitKey: true}]);
    return stream.pipe(parser);
}

function startParsing(_rdbConn) {
    getStream().on('data', function (data) {

        var latitude = []
        var longitude = []
        console.log(data.value.properties.ZCTA5CE10)

        data.value.geometry.coordinates.forEach(function (item) {
            latitude.push(item[0])
            longitude.push(item[1])
        })

        meow = {}
        var zip_code = data.value.properties.ZCTA5CE10
        var polygon = [[latitude.max(), longitude.min()], [latitude.max(), longitude.max()], [latitude.min(), longitude.max()], [latitude.min(), longitude.min()], [latitude.max(), longitude.min()]]
        meow[zip_code] = polygon
        console.log(meow)
        try {
            // var todo = yield parse(this);
            // todo.createdAt = r.now(); // Set the field `createdAt` to the current time
            var result = r.table('zip_code_polygon').insert({'zip_code': zip_code, 'polygon': polygon} , { returnChanges: true }).run(_rdbConn);

            // todo = result.changes[0].new_val; // todo now contains the previous todo + a field `id` and `createdAt`
            // this.body = JSON.stringify(todo);
        }
        catch (e) {
            this.status = 500;
            this.body = e.message || http.STATUS_CODES[this.status];
        }
        
    })

}


    // http://terminal2.expedia.com/x/geo/features?within=WKT:POLYGON((-67.786033 46.508813, -67.786033 46.599908, -67.908946 46.599908, -67.908946 46.508813, -67.786033 46.508813))&apikey=0b4WLhLx3EPmSPiE5bLyXieIsAL2szm3