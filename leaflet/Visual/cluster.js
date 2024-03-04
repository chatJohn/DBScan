var map = L.map('map', {
    center: [39.90741, 116.60394],
    zoom: 13,    
    preferCanvas: true
});
// 添加地图图层
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 19,
    attribution: 'Map data © <a href="https://openstreetmap.org">OpenStreetMap</a> contributors'
}).addTo(map);

// 读取并解析数据文件
fetch('2.txt') // 替换为你的数据文件路径

    .then(response => response.text())
    .then(data => {
        // 将文本数据按行分割为数组
        var lines = data.trim().split('\n');

        var clusters = {};
        lines.forEach(line => {
            var all = line.split(',');
			var spatiotime = all[0]
			var spatiotimeSplit = spatiotime.trim().slice(1, -1).split(",");
			var lng = parseFloat(spatiotimeSplit[0]);
			var lat = parseFloat(spatiotimeSplit[1]);
			var time = parseFloat(spatiotimeSplit[2]);
            // 检查该聚簇是否已经存在，如果不存在，则创建一个新的聚簇图层
            if (!clusters[cluster_id]) {
                clusters[cluster_id] = L.layerGroup().addTo(map);
            }

            // 创建标记并将其添加到相应的聚簇图层中
            var marker = L.marker([parseFloat(lat), parseFloat(lng)]).addTo(clusters[cluster_id]);
            
            // 可以根据聚簇 ID 设置不同的颜色
            var color = getColorForCluster(cluster_id);
            marker.setIcon(L.icon({
                iconSize: [80, 80],
				iconUrl:'images/marker-icon.png',
                // iconAnchor: [12, 41],
                // popupAnchor: [1, -34],
                shadowSize: [41, 41],
				shadowUrl: 'images/marker-shadow.png',
                // shadowAnchor: [12, 41],
                // 设置标记颜色
                iconColor: color
            }));
            marker.bindPopup('Cluster ID: ' + cluster_id);
        });
    })
    .catch(error => console.error('Error:', error));

var marker = L.marker([39.90741, 116.60394]).addTo(map);

            
function getColorForCluster(clusterId) {
    // 自定义聚簇 ID 和颜色的映射关系
    var colorMap = {
        0: 'blue',
		1: 'green',
		2: 'red',
		3: 'yellow',
		4: 'pink',
		5: 'cyan',
		6: 'orange',
		7: 'purple',
		8: 'white',
		9: 'black',
		10: 'brown',
		11: 'silver',
		12: 'gray',
		13: 'gold',
		14: 'turquoise',
		15: 'fuchsia',
		16: 'skyblue',
		17: 'darkblue',
		18: 'darkred',
		19: 'seagreen'
        // 添加更多的聚簇 ID 和颜色映射
    };
    return colorMap[clusterId] || 'black';
}

function onMapClick(e) {
		    alert("You clicked the map at " + e.latlng);
		}
map.on('click', onMapClick);
