var current = null;
current = {};
var prev = null;
prev = {};

function numberWithCommas(x) {
    return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}
function processPiDaily(data){
	current["piDailyData"] = sort_by_key(data, 'RecordDate');
}
function processPlantMonthly(data){
	current["plantMonthlyData"] = sort_by_key(data, 'RecordDate');
	var colKeys = [];
	for(var i = 0;i<data.length;i++){
		Object.keys(data[i]).forEach(function(key){
			if(colKeys.indexOf(key) == -1){
				colKeys.push(key);
			}
		});
	}
	for(var i = 0;i<colKeys.length-4;i++){
		current[colKeys[i+4]] = parseFloat(current["plantMonthlyData"][current["plantMonthlyData"].length-1][colKeys[i+4]]);
	}
	for(var i = 0;i<colKeys.length-4;i++){
		prev[colKeys[i+4]] = parseFloat(current["plantMonthlyData"][current["plantMonthlyData"].length-2][colKeys[i+4]]);
	}
	var dd = new Date(data[current["plantMonthlyData"].length-1]['RecordDate'])
	document.getElementById('plantDate').innerHTML = '[ ' + current["months"][dd.getMonth()] + '-' + dd.getFullYear() + ' ]';
}
function processFlareData(data){
	current["flareData"] = sort_by_key(data, 'RecordDate');
	var colKeys = [];
	for(var i = 0;i<data.length;i++){
		Object.keys(data[i]).forEach(function(key){
			if(colKeys.indexOf(key) == -1){
				colKeys.push(key);
			}
		});
	}
	for(var i = 0;i<colKeys.length-4;i++){
		current[colKeys[i+4]] = parseFloat(current["flareData"][current["flareData"].length-1][colKeys[i+4]]);
	}
	for(var i = 0;i<colKeys.length-4;i++){
		prev[colKeys[i+4]] = parseFloat(current["flareData"][current["flareData"].length-2][colKeys[i+4]]);
	}
	var dd = new Date(data[current["flareData"].length-1]['RecordDate'])
	document.getElementById('flareDate').innerHTML = '[ Week ' + weekNumber(dd) + ' ]';
}
function weekNumber(date){
  var firstJanuary = new Date(date.getFullYear(), 0, 1);
  var dayNr = Math.ceil((date - firstJanuary) / (24 * 60 * 60 * 1000));
  var weekNr = Math.ceil((dayNr + firstJanuary.getDay()) / 7);
  return weekNr;
}

//Get most recent data from PI database
function processLiveData(data){
	current["liveData"] = sort_by_key(data, 'RecordDate');
	//Get list of data columns
	var colKeys = [];
	for(var i = 0;i<data.length;i++){
		Object.keys(data[i]).forEach(function(key){
			if(colKeys.indexOf(key) == -1){
				colKeys.push(key);
			}
		});
	}
	console.log(new Date())

	for(var i = 0;i<colKeys.length-4;i++){
		current[colKeys[i+4]] = parseFloat(current["liveData"][current["liveData"].length-1][colKeys[i+4]]);
	}
	for(var i = 0;i<colKeys.length-4;i++){
		prev[colKeys[i+4]] = parseFloat(current["piDailyData"][current["piDailyData"].length-1][colKeys[i+4]]);
	}

	//parse Plant data to dashboard
	var cols1 = ['Acid_Gas_CO2','Trains_CO2e','LHU_CO2e','GTG_CO2e'];
	for(var i = 0;i<cols1.length;i++){
		if(prev[cols1[i]] == current[cols1[i]]){
			document.getElementById(cols1[i]).innerHTML = '<div class="circle" style="border-color: orange">'+
			'<div class="inner-circle" style="border-color: orange"><div style="margin-top: 30%;" id="' + 'd-'+cols1[i] + '" class="dataValue">' + 
			numberWithCommas(current[cols1[i]].toFixed()) + '</div></div></div>';
		}
		else if(prev[cols1[i]] < current[cols1[i]]){
			document.getElementById(cols1[i]).innerHTML = '<div class="circle" style="border-color: red">'+
			'<div class="inner-circle" style="border-color: red"><div style="margin-top: 30%;" id="' + 'd-'+cols1[i] + '" class="dataValue">' + 
			numberWithCommas(current[cols1[i]].toFixed()) + '</div></div></div>';
		}
		else if(prev[cols1[i]] > current[cols1[i]]){
			document.getElementById(cols1[i]).innerHTML = '<div class="circle" style="border-color: green">'+
			'<div class="inner-circle" style="border-color: green"><div style="margin-top: 30%;" id="' + 'd-'+cols1[i] + '" class="dataValue">' + 
			numberWithCommas(current[cols1[i]].toFixed()) + '</div></div></div>';
		}
	}
	//parse data to dashboard
	var cols2 = ['Waste_CO2e','Mobile_CO2e','Fugitives_CO2e'];
	for(var i = 0;i<cols2.length;i++){
		if(prev[cols2[i]] == current[cols2[i]]){
			document.getElementById(cols2[i]).innerHTML = '<span style="color: orange; font-size: 14pt; margin-left: 0%;">&#9673; </span>'+
			'<span id="' + 'mpl-'+cols2[i]+ '" class="dataValue">' + numberWithCommas(current[cols2[i]].toFixed()) + '</span><br>';
		}
		else if(prev[cols2[i]] < current[cols2[i]]){
			document.getElementById(cols2[i]).innerHTML = '<span style="color: red; font-size: 14pt; margin-left: 0%;">&#9650; </span>'+
			'<span id="' + 'mpl-'+cols2[i]+ '" class="dataValue">' + numberWithCommas(current[cols2[i]].toFixed()) + '</span><br>';
		}
		else if(prev[cols2[i]] > current[cols2[i]]){
			document.getElementById(cols2[i]).innerHTML = '<span style="color: green; font-size: 14pt; margin-left: 0%;">&#9660; </span>'+
			'<span id="' + 'mpl-'+cols2[i]+ '" class="dataValue">' + numberWithCommas(current[cols2[i]].toFixed()) + '</span><br>';
		}
	}
	var cols3 = ['Flare_CO2e'];
	for(var i = 0;i<cols3.length;i++){
		if(prev[cols3[i]] == current[cols3[i]]){
			document.getElementById(cols3[i]).innerHTML = '<span style="color: orange; font-size: 14pt; margin-left: 0%;">&#9673; </span>'+
			'<span id="' + 'wpl-'+cols3[i]+ '" class="dataValue">' + numberWithCommas(current[cols3[i]].toFixed()) + '</span><br>';
		}
		else if(prev[cols3[i]] < current[cols3[i]]){
			document.getElementById(cols3[i]).innerHTML = '<span style="color: red; font-size: 14pt; margin-left: 0%;">&#9650; </span>'+
			'<span id="' + 'wpl-'+cols3[i]+ '" class="dataValue">' + numberWithCommas(current[cols3[i]].toFixed()) + '</span><br>';
		}
		else if(prev[cols3[i]] > current[cols3[i]]){
			document.getElementById(cols3[i]).innerHTML = '<span style="color: green; font-size: 14pt; margin-left: 0%;">&#9660; </span>'+
			'<span id="' + 'wpl-'+cols3[i]+ '" class="dataValue">' + numberWithCommas(current[cols3[i]].toFixed()) + '</span><br>';
		}
	}
	$('#openPlant').on('click',function(){
		var thisTable = '<div style="margin-left: 70px;"><table border="1" style="text-align: center;"><tr><th style="width: 80px;">Trains</th>';
		for(var i = 1;i<7;i++){
			thisTable = thisTable + '<td style="width: 80px;">Train '+i+'</td>';			
		}
		thisTable = thisTable + '<td style="width: 80px;">Total</td></tr><tr><th>CO2 (T)</th>';
		for(var i = 1;i<7;i++){
			thisTable = thisTable + '<td id="' + 'd-'+'Acid_Gas_T'+i+'_CO2'+ '" class="dataValue">'+numberWithCommas(current['Acid_Gas_T'+i+'_CO2'].toFixed(2))+'</td>';			
		}
		thisTable = thisTable + '<td id="' + 'd-'+'Acid_Gas_CO2'+ '" class="dataValue">'+numberWithCommas(current['Acid_Gas_CO2'].toFixed(2))+'</td></tr></table></div>';
		document.getElementById('id06').style.display='block';
		var btns1 = ['Acid_Gas_Removal','Trains','LHU','GTG','Waste','Fugitives','Mobile','Flare'];
		for(var i = 0;i<btns1.length;i++){
			document. getElementById(btns1[i]).className = "innerBtns"; 
		}
		document.getElementById("Acid_Gas_Removal").className = "innerBtnSel";
		document.getElementById('plantDetails').innerHTML = thisTable;
	});
	$('#Trains').on('click',function(){
		var thisTable = '<div style="margin-left: 70px;"><table border="1" style="text-align: center;"><tr><th style="width: 80px;"></th>';
		for(var i = 1;i<7;i++){
			thisTable = thisTable + '<td style="width: 80px;">Train '+i+'</td>';			
		}
		thisTable = thisTable + '<td style="width: 80px;">Total</td></tr><tr><th>CO2 (T)</th>';
		for(var i = 1;i<7;i++){
			thisTable = thisTable + '<td id="' + 'd-'+'T'+i+'_CO2'+ '" class="dataValue">'+numberWithCommas(current['T'+i+'_CO2'].toFixed(2))+'</td>';
		}
		thisTable = thisTable + '<td id="' + 'd-'+'Trains_CO2'+ '" style="width: 80px;" class="dataValue">'+numberWithCommas(current['Trains_CO2'].toFixed(2))+'</td></tr><tr><th>N2O (Kg)</th>';
		for(var i = 1;i<7;i++){
			thisTable = thisTable + '<td id="' + 'd-'+'T'+i+'_N2O'+ '" class="dataValue">'+numberWithCommas((current['T'+i+'_N2O']*1000).toFixed(2))+'</td>';
		}
		thisTable = thisTable + '<td id="' + 'd-'+'Trains_N2O'+ '" style="width: 80px;" class="dataValue">'+numberWithCommas((current['Trains_N2O']*1000).toFixed(2))+'</td></tr><tr><th>CH4 (Kg)</th>';
		for(var i = 1;i<7;i++){
			thisTable = thisTable + '<td id="' + 'd-'+'T'+i+'_CH4'+ '" class="dataValue">'+numberWithCommas((current['T'+i+'_CH4']*1000).toFixed(2))+'</td>';
		}
		thisTable = thisTable + '<td id="' + 'd-'+'Trains_CH4'+ '" class="dataValue">'+numberWithCommas((current['Trains_CH4']*1000).toFixed(2))+'</td></tr></table></div>';
		document.getElementById('plantDetails').innerHTML = thisTable;

		var btns1 = ['Acid_Gas_Removal','Trains','LHU','GTG','Waste','Fugitives','Mobile','Flare'];
		for(var i = 0;i<btns1.length;i++){
			document. getElementById(btns1[i]).className = "innerBtns"; 
		}
		document.getElementById("Trains").className = "innerBtnSel"; 
	});
	$('#Acid_Gas_Removal').on('click',function(){
		var thisTable = '<div style="margin-left: 70px;"><table border="1" style="text-align: center;"><tr><th style="width: 80px;"></th>';
		for(var i = 1;i<7;i++){
			thisTable = thisTable + '<td style="width: 80px;">Train '+i+'</td>';			
		}
		thisTable = thisTable + '<td style="width: 80px;">Total</td></tr><tr><th>CO2 (T)</th>';
		for(var i = 1;i<7;i++){
			thisTable = thisTable + '<td id="' + 'd-'+'Acid_Gas_T'+i+'_CO2'+ '" class="dataValue">'+numberWithCommas(current['Acid_Gas_T'+i+'_CO2'].toFixed(2))+'</td>';			
		}
		thisTable = thisTable + '<td id="' + 'd-'+'Acid_Gas_CO2'+ '" class="dataValue">'+numberWithCommas(current['Acid_Gas_CO2'].toFixed(2))+'</td></tr></table></div>';
		document.getElementById('plantDetails').innerHTML = thisTable;

		var btns1 = ['Acid_Gas_Removal','Trains','LHU','GTG','Waste','Fugitives','Mobile','Flare'];
		for(var i = 0;i<btns1.length;i++){
			document. getElementById(btns1[i]).className = "innerBtns"; 
		}
		document.getElementById("Acid_Gas_Removal").className = "innerBtnSel"; 
	});
	var btns2 = ['LHU','GTG'];
	$(document.body).click(function(evt){
        var clicked = evt.target;
        var currentID = clicked.id || "No ID!";
        for(var i = 0;i<btns2.length;i++){
            if (currentID != "No ID!" && btns2[i] == currentID){
                var thisTable = '<div style="margin-left: 250px;"><table border="1" style="text-align: center;"><tr>'+
				'<th style="width: 100px;">CO2 (T)</th><th style="width: 100px;">N2O (Kg)</th><th style="width: 100px;">CH4 (Kg)</th></tr><tr>'+
				'<td id="' + 'd-'+currentID+'_CO2'+ '" class="dataValue">'+numberWithCommas(current[currentID+'_CO2'].toFixed(2))+'</td>'+
				'<td id="' + 'd-'+currentID+'_N2O'+ '" class="dataValue">'+numberWithCommas((current[currentID+'_N2O']*1000).toFixed(2))+'</td>'+
				'<td id="' + 'd-'+currentID+'_CH4'+ '" class="dataValue">'+numberWithCommas((current[currentID+'_CH4']*1000).toFixed(2))+'</td>'+
				'</tr></table></div>';
				document.getElementById('plantDetails').innerHTML = thisTable;
		
				var btns1 = ['Acid_Gas_Removal','Trains','LHU','GTG','Waste','Fugitives','Mobile','Flare'];
				for(var i = 0;i<btns1.length;i++){
					document. getElementById(btns1[i]).className = "innerBtns"; 
				}
				document.getElementById(currentID).className = "innerBtnSel";
                break;
            }
        }
    });
	var btns4 = ['Waste','Mobile'];
	$(document.body).click(function(evt){
        var clicked = evt.target;
        var currentID = clicked.id || "No ID!";
        for(var i = 0;i<btns4.length;i++){
            if (currentID != "No ID!" && btns4[i] == currentID){
                var thisTable = '<div style="margin-left: 250px;"><table border="1" style="text-align: center;"><tr>'+
				'<th style="width: 100px;">CO2 (T)</th><th style="width: 100px;">N2O (Kg)</th><th style="width: 100px;">CH4 (Kg)</th></tr><tr>'+
				'<td id="' + 'mpl-'+currentID+'_CO2'+ '" class="dataValue">'+numberWithCommas(current[currentID+'_CO2'].toFixed(2))+'</td>'+
				'<td id="' + 'mpl-'+currentID+'_N2O'+ '" class="dataValue">'+numberWithCommas((current[currentID+'_N2O']*1000).toFixed(2))+'</td>'+
				'<td id="' + 'mpl-'+currentID+'_CH4'+ '" class="dataValue">'+numberWithCommas((current[currentID+'_CH4']*1000).toFixed(2))+'</td>'+
				'</tr></table></div>';
				document.getElementById('plantDetails').innerHTML = thisTable;
		
				var btns1 = ['Acid_Gas_Removal','Trains','LHU','GTG','Waste','Fugitives','Mobile','Flare'];
				for(var i = 0;i<btns1.length;i++){
					document. getElementById(btns1[i]).className = "innerBtns"; 
				}
				document.getElementById(currentID).className = "innerBtnSel";
                break;
            }
        }
    });
	var btns5 = ['Flare'];
	$(document.body).click(function(evt){
        var clicked = evt.target;
        var currentID = clicked.id || "No ID!";
        for(var i = 0;i<btns5.length;i++){
            if (currentID != "No ID!" && btns5[i] == currentID){
                var thisTable = '<div style="margin-left: 250px;"><table border="1" style="text-align: center;"><tr>'+
				'<th style="width: 100px;">CO2 (T)</th><th style="width: 100px;">N2O (Kg)</th><th style="width: 100px;">CH4 (T)</th></tr><tr>'+
				'<td id="' + 'wpl-'+currentID+'_CO2'+ '" class="dataValue">'+numberWithCommas(current[currentID+'_CO2'].toFixed())+'</td>'+
				'<td id="' + 'wpl-'+currentID+'_N2O'+ '" class="dataValue">'+numberWithCommas((current[currentID+'_N2O']*1000).toFixed(2))+'</td>'+
				'<td id="' + 'wpl-'+currentID+'_CH4'+ '" class="dataValue">'+numberWithCommas((current[currentID+'_CH4']).toFixed(2))+'</td>'+
				'</tr></table></div>';
				document.getElementById('plantDetails').innerHTML = thisTable;
		
				var btns1 = ['Acid_Gas_Removal','Trains','LHU','GTG','Waste','Fugitives','Mobile','Flare'];
				for(var i = 0;i<btns1.length;i++){
					document. getElementById(btns1[i]).className = "innerBtns"; 
				}
				document.getElementById(currentID).className = "innerBtnSel";
                break;
            }
        }
    });
	var btns3 = ['Fugitives'];
	$(document.body).click(function(evt){
        var clicked = evt.target;
        var currentID = clicked.id || "No ID!";
        for(var i = 0;i<btns3.length;i++){
            if (currentID != "No ID!" && btns3[i] == currentID){
                var thisTable = '<div style="margin-left: 350px;"><table border="1" style="text-align: center;"><tr>'+
				'<th style="width: 100px;">CH4 (Kg)</th></tr><tr>'+
				'<td id="' + 'mpl-'+currentID+'_CH4'+ '" class="dataValue">'+numberWithCommas((current[currentID+'_CH4']*1000).toFixed())+'</td>'+
				'</tr></table></div>';
				document.getElementById('plantDetails').innerHTML = thisTable;
		
				var btns1 = ['Acid_Gas_Removal','Trains','LHU','GTG','Waste','Fugitives','Mobile','Flare'];
				for(var i = 0;i<btns1.length;i++){
					document. getElementById(btns1[i]).className = "innerBtns"; 
				}
				document.getElementById(currentID).className = "innerBtnSel";
                break;
            }
        }
    });
	$(document.body).click(function(evt){
        var clicked = evt.target;
        var currentID = clicked.id || "No ID!";
        if (currentID != "No ID!" && document.getElementById(currentID).className == 'dataValue' && currentID.split('-')[0] == 'd'){
			current["ID"] = currentID;
			var thisID = currentID.split('-')[1];
			var selMths = []
			var selData = []
			selMths.push('x')
			selData.push(thisID);

			for(var i = 0;i<365;i++){
				//var thisDate = addMonths(new Date(), i-12);
				var d = new Date();
				var thisDate = new Date(d.setDate(d.getDate()+i-365));
				var currDate = formatDate(thisDate)
				selMths.push(currDate)
				for(var j = 0;j<current["piDailyData"].length;j++){
					RecordDate = new Date(current["piDailyData"][j].RecordDate)
					if(RecordDate.getDate() == thisDate.getDate() && RecordDate.getMonth() == thisDate.getMonth() && RecordDate.getFullYear() == thisDate.getFullYear()){
						var currData = (parseFloat(current["piDailyData"][j][thisID])).toFixed(2);
						break;
					}
					else{
						var currData = 'null';
					}
				}
				selData.push(currData);
			}
			document.getElementById('startDate').value = selMths[1];
			document.getElementById('endDate').value = selMths[selMths.length-1];
			var chart = bb.generate({
				size: {
					height: 250,
					width: 560
				},
				data: {
					x: "x",
					columns: [
						selMths,
						selData
					],
					type: "line", // for ESM specify as: line()
				},
				axis: {
					x: {
						type: "timeseries",
						tick: {
							format: function(x) {
								return x.getDate() + '-' + months(x.getMonth()) + '-' + (String(x.getFullYear())).slice(-2);
							}
						},
						tick: {
							culling: {
							  max: 12
							}
						}
					},
					y: {
						tick: {
							culling: {
							  max: 4
							}
						}
					}
				},
				point: {
					pattern: [
					  "<circle cx='0' cy='0' r='0'></circle>"
					]
				},
				zoom: {
					enabled: true,
					type: "drag"
				},
				bindto: "#lineChart"
			});
			document.getElementById('id02').style.display='block';
		}
		else if (currentID != "No ID!" && document.getElementById(currentID).className == 'dataValue' && currentID.split('-')[0] == 'mpl'){
			current["ID"] = currentID;
			var thisID = currentID.split('-')[1];
			var selMths = []
			var selData = []
			selMths.push('x')
			selData.push(thisID);

			for(var i = 0;i<12;i++){
				var thisDate = addMonths(new Date(), i-12);
				var currDate = formatDate(thisDate)
				selMths.push(currDate);
				for(var j = 0;j<current["plantMonthlyData"].length;j++){
					RecordDate = new Date(current["plantMonthlyData"][j].RecordDate)
					if(RecordDate.getMonth() == thisDate.getMonth() && RecordDate.getFullYear() == thisDate.getFullYear()){
						var currData = (parseFloat(current["plantMonthlyData"][j][thisID])).toFixed(2);
						break;
					}
					else{
						var currData = 'null';
					}
				}
				selData.push(currData);
			}
			document.getElementById('startDate').value = selMths[1];
			document.getElementById('endDate').value = selMths[selMths.length-1];
			var chart = bb.generate({
				size: {
					height: 250,
					width: 560
				},
				data: {
					x: "x",
					columns: [
						selMths,
						selData
					],
					type: "line",
				},
				axis: {
					x: {
						type: "timeseries",
						tick: {
							format: function(x) {
								return months(x.getMonth()) + '-' + (String(x.getFullYear())).slice(-2);
							}
						}
					},
				},
				bindto: "#lineChart"
			});
			document.getElementById('id02').style.display='block';
		}
		else if (currentID != "No ID!" && document.getElementById(currentID).className == 'dataValue' && currentID.split('-')[0] == 'wpl'){
			current["ID"] = currentID;
			var thisID = currentID.split('-')[1];
			var selMths = []
			var selData = []
			selMths.push('x')
			selData.push(thisID);

			for(var i = 0;i<52;i++){
				//var thisDate = addMonths(new Date(), i-12);
				var d = new Date();
				var thisDate = new Date(d.setDate(d.getDate()+(i*7)-364));
				var currDate = formatDate(thisDate)
				for(var j = 0;j<current["flareData"].length;j++){
					RecordDate = new Date(current["flareData"][j].RecordDate)
					if(weekNumber(RecordDate) == weekNumber(thisDate) && RecordDate.getFullYear() == thisDate.getFullYear()){
						var currData = (parseFloat(current["flareData"][j][thisID])).toFixed(2);
						break;
					}
					else{
						var currData = 'null';
					}
				}
				selMths.push(formatDate(currDate))
				selData.push(currData);
			}
			document.getElementById('startDate').value = selMths[1];
			document.getElementById('endDate').value = selMths[selMths.length-1];
			var chart = bb.generate({
				size: {
					height: 250,
					width: 560
				},
				data: {
					x: "x",
					columns: [
						selMths,
						selData
					],
					type: "line", // for ESM specify as: line()
				},
				axis: {
					x: {
						type: "timeseries",
						tick: {
							format: function(x) {
								return 'Wk ' + weekNumber(x) + ', ' + (String(x.getFullYear())).slice(-2);
								//return x.getDate() + '-' + months(x.getMonth()) + '-' + (String(x.getFullYear())).slice(-2);
							}
						},
					},
					y: {
						tick: {
							culling: {
							  max: 4
							}
						}
					}
				},
				point: {
					pattern: [
					  "<circle cx='0' cy='0' r='0'></circle>"
					]
				},
				zoom: {
					enabled: true,
					type: "drag"
				},
				bindto: "#lineChart"
			});
			document.getElementById('id02').style.display='block';
		}
    });
}

function processOffData(data){
	current["offData"] = sort_by_key(data, 'RecordDate');
	var colKeys = [];
	for(var i = 0;i<data.length;i++){
		Object.keys(data[i]).forEach(function(key){
			if(colKeys.indexOf(key) == -1){
				colKeys.push(key);
			}
		});
	}
	for(var i = 0;i<colKeys.length-4;i++){
		current[colKeys[i+4]] = parseFloat(current["offData"][current["offData"].length-1][colKeys[i+4]]);
	}
}

function processOfficesData(data){
	current["officeData"] = sort_by_key(data, 'RecordDate');

	//Get list of data columns
	var colKeys = [];
	for(var i = 0;i<data.length;i++){
		Object.keys(data[i]).forEach(function(key){
			if(colKeys.indexOf(key) == -1){
				colKeys.push(key);
			}
		});
	}
	var dd = new Date(data[current["officeData"].length-1]['RecordDate'])
	document.getElementById('esdDate').innerHTML = '[ ' + current["months"][dd.getMonth()] + '-' + dd.getFullYear() + ' ]';

	for(var i = 0;i<colKeys.length-4;i++){
		current[colKeys[i+4]] = parseFloat(current["officeData"][current["officeData"].length-1][colKeys[i+4]]);
	}
	for(var i = 0;i<colKeys.length-4;i++){
		prev[colKeys[i+4]] = parseFloat(current["officeData"][current["officeData"].length-2][colKeys[i+4]]);
	}
	//parse ESD data to dashboard
	var cols1 = ['PHC_CO2e','ABJ_CO2e','LTO_CO2e','LON_NG_CO2e'];
	for(var i = 0;i<cols1.length;i++){
		if(prev[cols1[i]] == current[cols1[i]]){
			document.getElementById(cols1[i]).innerHTML = '<div class="circle" style="border-color: orange">'+
			'<div class="inner-circle" style="border-color: orange"><div style="margin-top: 30%;" id="' + 'moff-'+cols1[i]+ '" class="dataValue">' + 
			numberWithCommas(current[cols1[i]].toFixed(2)) + '</div></div></div>';
		}
		else if(prev[cols1[i]] < current[cols1[i]]){
			document.getElementById(cols1[i]).innerHTML = '<div class="circle" style="border-color: red">'+
			'<div class="inner-circle" style="border-color: red"><div style="margin-top: 30%;" id="' + 'moff-'+cols1[i]+ '" class="dataValue">' + 
			numberWithCommas(current[cols1[i]].toFixed(2)) + '</div></div></div>';
		}
		else if(prev[cols1[i]] > current[cols1[i]]){
			document.getElementById(cols1[i]).innerHTML = '<div class="circle" style="border-color: green">'+
			'<div class="inner-circle" style="border-color: green"><div style="margin-top: 30%;" id="' + 'moff-'+cols1[i]+ '" class="dataValue">' + 
			numberWithCommas(current[cols1[i]].toFixed(2)) + '</div></div></div>';
		}
	}
	$('#openOffice').on('click',function(){
		var thisTable = '<div style="margin-left: 140px;"><table border="1" style="text-align: center;"><tr>'+
			'<th style="width: 100px;"></th><th style="width: 100px;">Consumption</th><th></th><th style="width: 100px;">CO2 (T)</th></tr><tr>'+
			'<th style="width: 100px;">NG (MWh)</th>'+
			'<td id="' + 'mof-'+'PHC_NG'+ '" class="dataValue">'+numberWithCommas(current['PHC_NG'].toFixed(2))+'</td><td></td>'+
			'<td id="' + 'moff-'+'PHC_NG_CO2e'+ '" class="dataValue">'+numberWithCommas(current['PHC_NG_CO2e'].toFixed(2))+'</td></tr><tr>'+
			'<th style="width: 100px;">Diesel (L)</th>'+
			'<td id="' + 'mof-'+'PHC_Diesel'+ '" class="dataValue">'+numberWithCommas(current['PHC_Diesel'].toFixed(2))+'</td><td></td>'+
			'<td id="' + 'moff-'+'PHC_Diesel_CO2e'+ '" class="dataValue">'+numberWithCommas(current['PHC_Diesel_CO2e'].toFixed(2))+'</td></tr><tr>'+
			'</tr></table></div>';
		document.getElementById('officeDetails').innerHTML = thisTable;
		var btns4 = ['PHC','ABJ','LTO','LON'];
		for(var i = 0;i<btns4.length;i++){
			document. getElementById(btns4[i]).className = "innerBtns"; 
		}
		document.getElementById('PHC').className = "innerBtnSel";
		document.getElementById('id03').style.display='block';
	});
	var btns3 = ['PHC','ABJ','LTO'];
	$(document.body).click(function(evt){
        var clicked = evt.target;
        var currentID = clicked.id || "No ID!";
        for(var i = 0;i<btns3.length;i++){
            if (currentID != "No ID!" && btns3[i] == currentID){
                var thisTable = '<div style="margin-left: 140px;"><table border="1" style="text-align: center;"><tr>'+
					'<th style="width: 100px;"></th><th style="width: 100px;">Consumption</th><th></th><th style="width: 100px;">CO2 (T)</th></tr><tr>'+
					'<th style="width: 100px;">NG (MWh)</th>'+
					'<td id="' + 'mof-'+currentID+'_NG'+ '" class="dataValue">'+numberWithCommas(current[currentID+'_NG'].toFixed(2))+'</td><td></td>'+
					'<td id="' + 'moff-'+currentID+'_NG_CO2e'+ '" class="dataValue">'+numberWithCommas(current[currentID+'_NG_CO2e'].toFixed(2))+'</td></tr><tr>'+
					'<th style="width: 100px;">Diesel (L)</th>'+
					'<td id="' + 'mof-'+currentID+'_Diesel'+ '" class="dataValue">'+numberWithCommas(current[currentID+'_Diesel'].toFixed())+'</td><td></td>'+
					'<td id="' + 'moff-'+currentID+'_Diesel_CO2e'+ '" class="dataValue">'+numberWithCommas(current[currentID+'_Diesel_CO2e'].toFixed(2))+'</td></tr><tr>'+
					'</tr></table></div>';
				document.getElementById('officeDetails').innerHTML = thisTable;		
				var btns4 = ['PHC','ABJ','LTO','LON'];
				for(var i = 0;i<btns4.length;i++){
					document. getElementById(btns4[i]).className = "innerBtns"; 
				}
				document.getElementById(currentID).className = "innerBtnSel";
                break;
            }
        }
    });
	$('#LON').on('click',function(){
		var thisTable = '<div style="margin-left: 140px;"><table border="1" style="text-align: center;"><tr>'+
			'<th style="width: 100px;"></th><th style="width: 100px;">Consumption</th><th></th><th style="width: 100px;">CO2 (T)</th></tr><tr>'+
			'<th style="width: 100px;">NG (MWh)</th>'+
			'<td id="' + 'mof-'+'LON_NG'+ '" class="dataValue">'+numberWithCommas(current['LON_NG'].toFixed(2))+'</td><td></td>'+
			'<td id="' + 'moff-'+'LON_NG_CO2e'+ '" class="dataValue">'+numberWithCommas(current['LON_NG_CO2e'].toFixed(2))+'</td></tr><tr>'+
			'</tr></table></div>';
		document.getElementById('officeDetails').innerHTML = thisTable;
		var btns4 = ['PHC','ABJ','LTO','LON'];
		for(var i = 0;i<btns4.length;i++){
			document. getElementById(btns4[i]).className = "innerBtns"; 
		}
		document.getElementById('LON').className = "innerBtnSel";
	});
	$(document.body).click(function(evt){
        var clicked = evt.target;
        var currentID = clicked.id || "No ID!";
        if (currentID != "No ID!" && document.getElementById(currentID).className == 'dataValue' && currentID.split('-')[0] == 'moff'){
			current["ID"] = currentID;
			var thisID = currentID.split('-')[1];
			var selMths = []
			var selData = []
			selMths.push('x')
			selData.push(thisID);

			for(var i = 0;i<12;i++){
				var thisDate = addMonths(new Date(), i-12);
				var currDate = formatDate(thisDate)
				selMths.push(currDate);
				for(var j = 0;j<current["officeData"].length;j++){
					RecordDate = new Date(current["officeData"][j].RecordDate)
					if(RecordDate.getMonth() == thisDate.getMonth() && RecordDate.getFullYear() == thisDate.getFullYear()){
						var currData = (parseFloat(current["officeData"][j][thisID])).toFixed(2);
						break;
					}
					else{
						var currData = 'null';
					}
				}
				selData.push(currData);
			}
			document.getElementById('startDate').value = selMths[1];
			document.getElementById('endDate').value = selMths[selMths.length-1];
			var chart = bb.generate({
				size: {
					height: 250,
					width: 560
				},
				data: {
					x: "x",
					columns: [
						selMths,
						selData
					],
					type: "line",
				},
				axis: {
					x: {
						type: "timeseries",
						tick: {
							format: function(x) {
								return months(x.getMonth()) + '-' + (String(x.getFullYear())).slice(-2);
							}
						}
					},
				},
				bindto: "#lineChart"
			});
			document.getElementById('id02').style.display='block';
		}
		else if (currentID != "No ID!" && document.getElementById(currentID).className == 'dataValue' && currentID.split('-')[0] == 'mof'){
			current["ID"] = currentID;
			var thisID = currentID.split('-')[1];
			var selMths = []
			var selData = []
			selMths.push('x')
			selData.push(thisID);

			for(var i = 0;i<12;i++){
				var thisDate = addMonths(new Date(), i-12);
				var currDate = formatDate(thisDate)
				selMths.push(currDate);
				for(var j = 0;j<current["offData"].length;j++){
					RecordDate = new Date(current["offData"][j].RecordDate)
					if(RecordDate.getMonth() == thisDate.getMonth() && RecordDate.getFullYear() == thisDate.getFullYear()){
						var currData = (parseFloat(current["offData"][j][thisID])).toFixed(2);
						break;
					}
					else{
						var currData = 'null';
					}
				}
				selData.push(currData);
			}
			document.getElementById('startDate').value = selMths[1];
			document.getElementById('endDate').value = selMths[selMths.length-1];
			var chart = bb.generate({
				size: {
					height: 250,
					width: 560
				},
				data: {
					x: "x",
					columns: [
						selMths,
						selData
					],
					type: "line",
				},
				axis: {
					x: {
						type: "timeseries",
						tick: {
							format: function(x) {
								return months(x.getMonth()) + '-' + (String(x.getFullYear())).slice(-2);
							}
						}
					},
				},
				bindto: "#lineChart"
			});
			document.getElementById('id02').style.display='block';
		}
    });
}

function processLogisticsData(data){
	current["logisticsData"] = sort_by_key(data, 'RecordDate');

	//Get list of data columns
	var colKeys = [];
	for(var i = 0;i<data.length;i++){
		Object.keys(data[i]).forEach(function(key){
			if(colKeys.indexOf(key) == -1){
				colKeys.push(key);
			}
		});
	}
	var dd = new Date(data[current["logisticsData"].length-1]['RecordDate'])
	document.getElementById('lssDate').innerHTML = '[ ' + current["months"][dd.getMonth()] + '-' + dd.getFullYear() + ' ]';

	for(var i = 0;i<colKeys.length-4;i++){
		current[colKeys[i+4]] = parseFloat(current["logisticsData"][current["logisticsData"].length-1][colKeys[i+4]]);
	}

	for(var i = 0;i<colKeys.length-4;i++){
		prev[colKeys[i+4]] = parseFloat(current["logisticsData"][current["logisticsData"].length-2][colKeys[i+4]]);
	}
	//parse LSS data to dashboard
	var cols1 = ['Aviation_CO2e','Passenger_Boats_CO2e','Tug_Boats_CO2e','Escort_CO2e','Bny_Fleet_CO2e','Non_Bny_Fleet_CO2e'];
	for(var i = 0;i<cols1.length;i++){
		if(prev[cols1[i]] == current[cols1[i]]){
			document.getElementById(cols1[i]).innerHTML = '<span style="color: orange; font-size: 14pt; margin-left: 2%;">&#9673; </span>'+
			'<span id="' + 'mlss-'+cols1[i]+ '" class="dataValue">' + numberWithCommas(current[cols1[i]].toFixed(2)) + '</span>';
		}
		else if(prev[cols1[i]] < current[cols1[i]]){
			document.getElementById(cols1[i]).innerHTML = '<span style="color: red; font-size: 14pt; margin-left: 2%;">&#9650; </span>'+
			'<span id="' + 'mlss-'+cols1[i]+ '" class="dataValue">' + numberWithCommas(current[cols1[i]].toFixed(2)) + '</span>';
		}
		else if(prev[cols1[i]] > current[cols1[i]]){
			document.getElementById(cols1[i]).innerHTML = '<span style="color: green; font-size: 14pt; margin-left: 2%;">&#9660; </span>'+
			'<span id="' + 'mlss-'+cols1[i]+ '" class="dataValue">' + numberWithCommas(current[cols1[i]].toFixed(2)) + '</span>';
		}
	}
	$('#openLSS').on('click',function(){
		var thisTable = '<div style="margin-left: 140px;"><table border="1" style="text-align: center;"><tr>'+
			'<th style="width: 150px;">CO2 Per Passenger (T)</th><th style="width: 150px;">CO2 Per Distance (T)</th></tr><tr>'+
			'<td id="' + 'mlss-'+'Av_Per_Passenger'+ '" class="dataValue">'+numberWithCommas(current['Av_Per_Passenger'].toFixed(2))+'</td>'+
			'<td id="' + 'mlss-'+'Av_Per_Distance'+ '" class="dataValue">'+numberWithCommas(current['Av_Per_Distance'].toFixed(2))+'</td></tr><tr>'+
			'</tr></table></div>';
		document.getElementById('lssDetails').innerHTML = thisTable;
		var btns4 = ['Av','P_Boats','T_Boats','Escort','Bny_Fleet','Non_Bny_Fleet'];
		for(var i = 0;i<btns4.length;i++){
			document. getElementById(btns4[i]).className = "innerBtns"; 
		}
		document.getElementById('Av').className = "innerBtnSel";
		document.getElementById('id07').style.display='block';
	});
	var btns5 = ['Av','P_Boats','T_Boats','Bny_Fleet','Non_Bny_Fleet'];
	$(document.body).click(function(evt){
        var clicked = evt.target;
        var currentID = clicked.id || "No ID!";
        for(var i = 0;i<btns5.length;i++){
            if (currentID != "No ID!" && btns5[i] == currentID){
                var thisTable = '<div style="margin-left: 140px;"><table border="1" style="text-align: center;"><tr>'+
				'<th style="width: 150px;">CO2 Per Passenger (T)</th><th style="width: 150px;">CO2 Per Distance (T)</th></tr><tr>'+
				'<td id="' + 'mlss-'+currentID+'_Per_Passenger'+ '" class="dataValue">'+numberWithCommas(current[currentID+'_Per_Passenger'].toFixed(2))+'</td>'+
				'<td id="' + 'mlss-'+currentID+'_Per_Distance'+ '" class="dataValue">'+numberWithCommas(current[currentID+'_Per_Distance'].toFixed(2))+'</td></tr><tr>'+
				'</tr></table></div>';
				document.getElementById('lssDetails').innerHTML = thisTable;	
				var btns6 = ['Av','P_Boats','T_Boats','Escort','Bny_Fleet','Non_Bny_Fleet'];
				for(var i = 0;i<btns6.length;i++){
					document. getElementById(btns6[i]).className = "innerBtns"; 
				}
				document.getElementById(currentID).className = "innerBtnSel";
                break;
            }
        }
    });
	$('#Escort').on('click',function(){
		var thisTable = '<div style="margin-left: 70px;"><table border="1" style="text-align: center;"><tr>'+
		'<th style="width: 150px;"></th><th style="width: 150px;">CO2 Per Passenger (T)</th><th style="width: 150px;">CO2 Per Distance (T)</th></tr><tr>'+
		'<td>Long Range Escort</td>'+
		'<td id="' + 'mlss-'+'L_Escort_Per_Passenger'+ '" class="dataValue">'+numberWithCommas(current['L_Escort_Per_Passenger'].toFixed(2))+'</td>'+
		'<td id="' + 'mlss-'+'L_Escort_Per_Distance'+ '" class="dataValue">'+numberWithCommas(current['L_Escort_Per_Distance'].toFixed(2))+'</td></tr><tr>'+
		'<td>Passenger Escort</td>'+
		'<td id="' + 'mlss-'+'P_Escort_Per_Passenger'+ '" class="dataValue">'+numberWithCommas(current['P_Escort_Per_Passenger'].toFixed(2))+'</td>'+
		'<td id="' + 'mlss-'+'P_Escort_Per_Distance'+ '" class="dataValue">'+numberWithCommas(current['P_Escort_Per_Distance'].toFixed(2))+'</td></tr><tr>'+
		'</tr></table></div>';
		document.getElementById('lssDetails').innerHTML = thisTable;	
		var btns6 = ['Av','P_Boats','T_Boats','Escort','Bny_Fleet','Non_Bny_Fleet'];
		for(var i = 0;i<btns6.length;i++){
			document. getElementById(btns6[i]).className = "innerBtns"; 
		}
		document.getElementById('Escort').className = "innerBtnSel";
	});
	$(document.body).click(function(evt){
        var clicked = evt.target;
        var currentID = clicked.id || "No ID!";
        if (currentID != "No ID!" && document.getElementById(currentID).className == 'dataValue' && currentID.split('-')[0] == 'mlss'){
			current["ID"] = currentID;
			var thisID = currentID.split('-')[1];
			var selMths = []
			var selData = []
			selMths.push('x')
			selData.push(thisID);

			for(var i = 0;i<12;i++){
				var thisDate = addMonths(new Date(), i-12);
				var currDate = formatDate(thisDate)
				selMths.push(currDate);
				for(var j = 0;j<current["logisticsData"].length;j++){
					RecordDate = new Date(current["logisticsData"][j].RecordDate)
					if(RecordDate.getMonth() == thisDate.getMonth() && RecordDate.getFullYear() == thisDate.getFullYear()){
						var currData = (parseFloat(current["logisticsData"][j][thisID])).toFixed(2);
						break;
					}
					else{
						var currData = 'null';
					}
				}
				selData.push(currData);
			}
			document.getElementById('startDate').value = selMths[1];
			document.getElementById('endDate').value = selMths[selMths.length-1];
			var chart = bb.generate({
				size: {
					height: 250,
					width: 560
				},
				data: {
					x: "x",
					columns: [
						selMths,
						selData
					],
					type: "line",
				},
				axis: {
					x: {
						type: "timeseries",
						tick: {
							format: function(x) {
								return months(x.getMonth()) + '-' + (String(x.getFullYear())).slice(-2);
							}
						}
					},
				},
				bindto: "#lineChart"
			});
			document.getElementById('id02').style.display='block';
		}	
	});
}
function processSummaryData(data){
	current["summaryData"] = sort_by_key(data, 'RecordDate');

	//Get list of data columns
	var colKeys = [];
	for(var i = 0;i<data.length;i++){
		Object.keys(data[i]).forEach(function(key){
			if(colKeys.indexOf(key) == -1){
				colKeys.push(key);
			}
		});
	}
	var dd = new Date(data[current["summaryData"].length-1]['RecordDate'])
	//document.getElementById('summDate').innerHTML = '[ ' + current["months"][dd.getMonth()] + '-' + dd.getFullYear() + ' ]';

	for(var i = 0;i<colKeys.length-4;i++){
		//current[colKeys[i+4]] = parseFloat(current["summaryData"][current["summaryData"].length-1][colKeys[i+4]]);
		//prev[colKeys[i+4]] = parseFloat(current["summaryData"][current["summaryData"].length-2][colKeys[i+4]]);

		for(var j = 0;j<current["summaryData"].length;j++){
			if(current["summaryData"][current["summaryData"].length-1-j][colKeys[i+4]] != 'nan'){
				current[colKeys[i+4]] = parseFloat(current["summaryData"][current["summaryData"].length-1-j][colKeys[i+4]]);
				prev[colKeys[i+4]] = parseFloat(current["summaryData"][current["summaryData"].length-2-j][colKeys[i+4]]);
				current[colKeys[i+4]+'_Date'] = current["summaryData"][current["summaryData"].length-1-j]['RecordDate'];
				break;
			}
		}
	}

	//parse summary data to dashboard
	var cols1 = ['Upstream_CO2e','Plant_CO2e','Shipping_CO2e','Logistics_CO2e','Office_CO2e','Project_CO2e'];
	for(var i = 0;i<cols1.length;i++){
		var dd = new Date(current[cols1[i]+'_Date'])
		document.getElementById(cols1[i]+'_Date').innerHTML = '[ ' + current["months"][dd.getMonth()] + '-' + dd.getFullYear() + ' ]';
		if(prev[cols1[i]] == current[cols1[i]]){
			document.getElementById(cols1[i]).innerHTML = '<span style="color: orange; font-size: 30pt; margin-left: 0%;">&#9673; </span>'+
			'<span id="' + 'msumm-'+cols1[i]+ '" class="dataValue2">' + numberWithCommas(current[cols1[i]].toFixed()) + '</span>';
		}
		else if(prev[cols1[i]] < current[cols1[i]]){
			document.getElementById(cols1[i]).innerHTML = '<span style="color: red; font-size: 30pt; margin-left: 0%;">&#9650; </span>'+
			'<span id="' + 'msumm-'+cols1[i]+ '" class="dataValue2">' + numberWithCommas(current[cols1[i]].toFixed()) + '</span>';
		}
		else if(prev[cols1[i]] > current[cols1[i]]){
			document.getElementById(cols1[i]).innerHTML = '<span style="color: green; font-size: 30pt; margin-left: 0%;">&#9660; </span>'+
			'<span id="' + 'msumm-'+cols1[i]+ '" class="dataValue2">' + numberWithCommas(current[cols1[i]].toFixed()) + '</span>';
		}
	}
	var cols2 = ['CO2e_Per_LNG','CO2e_Per_LNG_Lag','CO2e_Per_Prod_Lag']
	for(var i = 0;i<cols2.length;i++){
		var dd = new Date(current[cols2[i]+'_Date'])
		document.getElementById(cols2[i]+'_Date').innerHTML = '[ ' + current["months"][dd.getMonth()] + '-' + dd.getFullYear() + ' ]';
		document.getElementById(cols2[i]).innerHTML = numberWithCommas(current[cols2[i]].toFixed(2));
	}
	var dd2 = new Date(current['CO2e_Per_LNG_Date'])
	document.getElementById('CO2e_Per_Prod_Date').innerHTML = '[ ' + current["months"][dd2.getMonth()] + '-' + dd2.getFullYear() + ' ]';
	document.getElementById('CO2e_Per_LNG2').innerHTML = numberWithCommas(current['CO2e_Per_LNG'].toFixed(2));
	document.getElementById('CO2e_Per_LNG_Lag2').innerHTML = numberWithCommas(current['CO2e_Per_LNG_Lag'].toFixed(2));
	document.getElementById('CO2e_Per_Prod_Lag2').innerHTML = numberWithCommas(current['CO2e_Per_Prod_Lag'].toFixed(2));

	document.getElementById('summMonth').value = '';
	/*$('#openLSS').on('click',function(){
		var thisTable = '<div style="margin-left: 140px;"><table border="1" style="text-align: center;"><tr>'+
			'<th style="width: 150px;">CO2 Per Passenger (T)</th><th style="width: 150px;">CO2 Per Distance (T)</th></tr><tr>'+
			'<td id="' + 'mlss-'+'Av_Per_Passenger'+ '" class="dataValue">'+numberWithCommas(current['Av_Per_Passenger'].toFixed(2))+'</td>'+
			'<td id="' + 'mlss-'+'Av_Per_Distance'+ '" class="dataValue">'+numberWithCommas(current['Av_Per_Distance'].toFixed(2))+'</td></tr><tr>'+
			'</tr></table></div>';
		document.getElementById('lssDetails').innerHTML = thisTable;
		var btns4 = ['Av','P_Boats','T_Boats','Escort','Bny_Fleet','Non_Bny_Fleet'];
		for(var i = 0;i<btns4.length;i++){
			document. getElementById(btns4[i]).className = "innerBtns"; 
		}
		document.getElementById('Av').className = "innerBtnSel";
		document.getElementById('id07').style.display='block';
	});
	var btns5 = ['Av','P_Boats','T_Boats','Bny_Fleet','Non_Bny_Fleet'];
	$(document.body).click(function(evt){
        var clicked = evt.target;
        var currentID = clicked.id || "No ID!";
        for(var i = 0;i<btns5.length;i++){
            if (currentID != "No ID!" && btns5[i] == currentID){
                var thisTable = '<div style="margin-left: 140px;"><table border="1" style="text-align: center;"><tr>'+
				'<th style="width: 150px;">CO2 Per Passenger (T)</th><th style="width: 150px;">CO2 Per Distance (T)</th></tr><tr>'+
				'<td id="' + 'mlss-'+currentID+'_Per_Passenger'+ '" class="dataValue">'+numberWithCommas(current[currentID+'_Per_Passenger'].toFixed(2))+'</td>'+
				'<td id="' + 'mlss-'+currentID+'_Per_Distance'+ '" class="dataValue">'+numberWithCommas(current[currentID+'_Per_Distance'].toFixed(2))+'</td></tr><tr>'+
				'</tr></table></div>';
				document.getElementById('lssDetails').innerHTML = thisTable;	
				var btns6 = ['Av','P_Boats','T_Boats','Escort','Bny_Fleet','Non_Bny_Fleet'];
				for(var i = 0;i<btns6.length;i++){
					document. getElementById(btns6[i]).className = "innerBtns"; 
				}
				document.getElementById(currentID).className = "innerBtnSel";
                break;
            }
        }
    });
	$('#Escort').on('click',function(){
		var thisTable = '<div style="margin-left: 70px;"><table border="1" style="text-align: center;"><tr>'+
		'<th style="width: 150px;"></th><th style="width: 150px;">CO2 Per Passenger (T)</th><th style="width: 150px;">CO2 Per Distance (T)</th></tr><tr>'+
		'<td>Long Range Escort</td>'+
		'<td id="' + 'mlss-'+'L_Escort_Per_Passenger'+ '" class="dataValue">'+numberWithCommas(current['L_Escort_Per_Passenger'].toFixed(2))+'</td>'+
		'<td id="' + 'mlss-'+'L_Escort_Per_Distance'+ '" class="dataValue">'+numberWithCommas(current['L_Escort_Per_Distance'].toFixed(2))+'</td></tr><tr>'+
		'<td>Passenger Escort</td>'+
		'<td id="' + 'mlss-'+'P_Escort_Per_Passenger'+ '" class="dataValue">'+numberWithCommas(current['P_Escort_Per_Passenger'].toFixed(2))+'</td>'+
		'<td id="' + 'mlss-'+'P_Escort_Per_Distance'+ '" class="dataValue">'+numberWithCommas(current['P_Escort_Per_Distance'].toFixed(2))+'</td></tr><tr>'+
		'</tr></table></div>';
		document.getElementById('lssDetails').innerHTML = thisTable;	
		var btns6 = ['Av','P_Boats','T_Boats','Escort','Bny_Fleet','Non_Bny_Fleet'];
		for(var i = 0;i<btns6.length;i++){
			document. getElementById(btns6[i]).className = "innerBtns"; 
		}
		document.getElementById('Escort').className = "innerBtnSel";
	});*/
	$(document.body).click(function(evt){
        var clicked = evt.target;
        var currentID = clicked.id || "No ID!";
        if (currentID != "No ID!" && document.getElementById(currentID).className == 'dataValue2' && currentID.split('-')[0] == 'msumm'){
			current["ID"] = currentID;
			var thisID = currentID.split('-')[1];
			var selMths = []
			var selData = []
			selMths.push('x')
			selData.push(thisID);

			for(var i = 0;i<12;i++){
				var thisDate = addMonths(new Date(), i-12);
				var currDate = formatDate(thisDate)
				selMths.push(currDate);
				for(var j = 0;j<current["summaryData"].length;j++){
					RecordDate = new Date(current["summaryData"][j].RecordDate)
					if(RecordDate.getMonth() == thisDate.getMonth() && RecordDate.getFullYear() == thisDate.getFullYear()){
						var currData = (parseFloat(current["summaryData"][j][thisID])).toFixed(2);
						break;
					}
					else{
						var currData = 'null';
					}
				}
				selData.push(currData);
			}
			document.getElementById('startDate').value = selMths[1];
			document.getElementById('endDate').value = selMths[selMths.length-1];
			var chart = bb.generate({
				size: {
					height: 250,
					width: 560
				},
				data: {
					x: "x",
					columns: [
						selMths,
						selData
					],
					type: "line",
				},
				axis: {
					x: {
						type: "timeseries",
						tick: {
							format: function(x) {
								return months(x.getMonth()) + '-' + (String(x.getFullYear())).slice(-2);
							}
						}
					},
				},
				bindto: "#lineChart"
			});
			document.getElementById('id02').style.display='block';
		}	
	});
}
function processprojData(data){
	current["projData"] = sort_by_key(data, 'RecordDate');
	var colKeys = [];
	for(var i = 0;i<data.length;i++){
		Object.keys(data[i]).forEach(function(key){
			if(colKeys.indexOf(key) == -1){
				colKeys.push(key);
			}
		});
	}
	for(var i = 0;i<colKeys.length-4;i++){
		current[colKeys[i+4]] = parseFloat(current["projData"][current["projData"].length-1][colKeys[i+4]]);
	}
}
function processprojectData(data){
	current["projectsData"] = sort_by_key(data, 'RecordDate');

	//Get list of data columns
	var colKeys = [];
	for(var i = 0;i<data.length;i++){
		Object.keys(data[i]).forEach(function(key){
			if(colKeys.indexOf(key) == -1){
				colKeys.push(key);
			}
		});
	}
	var dd = new Date(data[current["projectsData"].length-1]['RecordDate'])
	document.getElementById('projDate').innerHTML = '[ ' + current["months"][dd.getMonth()] + '-' + dd.getFullYear() + ' ]';

	for(var i = 0;i<colKeys.length-4;i++){
		current[colKeys[i+4]] = parseFloat(current["projectsData"][current["projectsData"].length-1][colKeys[i+4]]);
	}

	for(var i = 0;i<colKeys.length-4;i++){
		prev[colKeys[i+4]] = parseFloat(current["projectsData"][current["projectsData"].length-2][colKeys[i+4]]);
	}
	//parse Projects data to dashboard
	var cols1 = ['Projects_CO2e'];
	for(var i = 0;i<cols1.length;i++){
		if(prev[cols1[i]] == current[cols1[i]]){
			document.getElementById(cols1[i]).innerHTML = '<span style="color: orange; font-size: 14pt; margin-left: 30%;">&#9673; </span>'+
			'<span id="' + 'mp-'+cols1[i]+ '" class="dataValue">' + numberWithCommas(current[cols1[i]].toFixed(2)) + '</span>';
		}
		else if(prev[cols1[i]] < current[cols1[i]]){
			document.getElementById(cols1[i]).innerHTML = '<span style="color: red; font-size: 14pt; margin-left: 30%;">&#9650; </span>'+
			'<span id="' + 'mp-'+cols1[i]+ '" class="dataValue">' + numberWithCommas(current[cols1[i]].toFixed(2)) + '</span>';
		}
		else if(prev[cols1[i]] > current[cols1[i]]){
			document.getElementById(cols1[i]).innerHTML = '<span style="color: green; font-size: 14pt; margin-left: 30%;">&#9660; </span>'+
			'<span id="' + 'mp-'+cols1[i]+ '" class="dataValue">' + numberWithCommas(current[cols1[i]].toFixed(2)) + '</span>';
		}
	}
	$('#openProjects').on('click',function(){
		var thisTable = '<div style="margin-left: 130px;"><table border="1" style="text-align: center;"><tr>'+
			'<th style="width: 100px;"></th><th style="width: 100px;">Consumption (L)</th><th></th><th style="width: 100px;">CO2e (T)</th></tr><tr>'+
			'<th style="width: 100px;">Diesel</th>'+
			'<td id="' + 'mpr-'+'Diesel'+ '" class="dataValue">'+numberWithCommas(current['Diesel'].toFixed())+'</td><td></td>'+
			'<td id="' + 'mp-'+'Diesel_CO2e'+ '" class="dataValue">'+numberWithCommas(current['Diesel_CO2e'].toFixed(2))+'</td></tr><tr>'+
			'<th style="width: 100px;">Petrol</th>'+
			'<td id="' + 'mpr-'+'Petrol'+ '" class="dataValue">'+numberWithCommas(current['Petrol'].toFixed())+'</td><td></td>'+
			'<td id="' + 'mp-'+'Petrol_CO2e'+ '" class="dataValue">'+numberWithCommas(current['Petrol_CO2e'].toFixed(2))+'</td></tr><tr>'+
			'</tr></table></div>';
		document.getElementById('projectDetails').innerHTML = thisTable;
		document.getElementById('id05').style.display='block';
	});
	$(document.body).click(function(evt){
        var clicked = evt.target;
        var currentID = clicked.id || "No ID!";
        if (currentID != "No ID!" && document.getElementById(currentID).className == 'dataValue' && currentID.split('-')[0] == 'mp'){
			current["ID"] = currentID;
			var thisID = currentID.split('-')[1];
			var selMths = []
			var selData = []
			selMths.push('x')
			selData.push(thisID);

			for(var i = 0;i<12;i++){
				var thisDate = addMonths(new Date(), i-12);
				var currDate = formatDate(thisDate)
				selMths.push(currDate);
				for(var j = 0;j<current["projectsData"].length;j++){
					RecordDate = new Date(current["projectsData"][j].RecordDate)
					if(RecordDate.getMonth() == thisDate.getMonth() && RecordDate.getFullYear() == thisDate.getFullYear()){
						var currData = (parseFloat(current["projectsData"][j][thisID])).toFixed(2);
						break;
					}
					else{
						var currData = 'null';
					}
				}
				selData.push(currData);
			}
			document.getElementById('startDate').value = selMths[1];
			document.getElementById('endDate').value = selMths[selMths.length-1];
			var chart = bb.generate({
				size: {
					height: 250,
					width: 560
				},
				data: {
					x: "x",
					columns: [
						selMths,
						selData
					],
					type: "line",
				},
				axis: {
					x: {
						type: "timeseries",
						tick: {
							format: function(x) {
								return months(x.getMonth()) + '-' + (String(x.getFullYear())).slice(-2);
							}
						}
					},
				},
				bindto: "#lineChart"
			});
			document.getElementById('id02').style.display='block';
		}
		if (currentID != "No ID!" && document.getElementById(currentID).className == 'dataValue' && currentID.split('-')[0] == 'mpr'){
			current["ID"] = currentID;
			var thisID = currentID.split('-')[1];
			var selMths = []
			var selData = []
			selMths.push('x')
			selData.push(thisID);

			for(var i = 0;i<12;i++){
				var thisDate = addMonths(new Date(), i-12);
				var currDate = formatDate(thisDate)
				selMths.push(currDate);
				for(var j = 0;j<current["projData"].length;j++){
					RecordDate = new Date(current["projData"][j].RecordDate)
					if(RecordDate.getMonth() == thisDate.getMonth() && RecordDate.getFullYear() == thisDate.getFullYear()){
						var currData = (parseFloat(current["projData"][j][thisID])).toFixed(2);
						break;
					}
					else{
						var currData = 'null';
					}
				}
				selData.push(currData);
			}
			document.getElementById('startDate').value = selMths[1];
			document.getElementById('endDate').value = selMths[selMths.length-1];
			var chart = bb.generate({
				size: {
					height: 250,
					width: 560
				},
				data: {
					x: "x",
					columns: [
						selMths,
						selData
					],
					type: "line",
				},
				axis: {
					x: {
						type: "timeseries",
						tick: {
							format: function(x) {
								return months(x.getMonth()) + '-' + (String(x.getFullYear())).slice(-2);
							}
						}
					},
				},
				bindto: "#lineChart"
			});
			document.getElementById('id02').style.display='block';
		}	
	});
}

function processUpstreamData(data){
	current["upstreamData"] = sort_by_key(data, 'RecordDate');

	//Get list of data columns
	var colKeys = [];
	for(var i = 0;i<data.length;i++){
		Object.keys(data[i]).forEach(function(key){
			if(colKeys.indexOf(key) == -1){
				colKeys.push(key);
			}
		});
	}
	var dd = new Date(data[current["upstreamData"].length-1]['RecordDate'])
	document.getElementById('upstreamDate').innerHTML = '[ ' + current["months"][dd.getMonth()] + '-' + dd.getFullYear() + ' ]';

	for(var i = 0;i<colKeys.length-4;i++){
		current[colKeys[i+4]] = parseFloat(current["upstreamData"][current["upstreamData"].length-1][colKeys[i+4]]);
	}

	for(var i = 0;i<colKeys.length-4;i++){
		prev[colKeys[i+4]] = parseFloat(current["upstreamData"][current["upstreamData"].length-2][colKeys[i+4]]);
	}
	//parse Projects data to dashboard
	var cols1 = ['SPDC_CO2e','TEPNG_CO2e','NAOC_CO2e'];
	for(var i = 0;i<cols1.length;i++){
		if(prev[cols1[i]] == current[cols1[i]]){
			document.getElementById(cols1[i]).innerHTML = '<span style="color: orange; font-size: 14pt; margin-left: 10%;">&#9673; </span>'+
			'<span id="' + 'mup-'+cols1[i]+ '" class="dataValue">' + numberWithCommas(current[cols1[i]].toFixed()) + '</span><br>';
		}
		else if(prev[cols1[i]] < current[cols1[i]]){
			document.getElementById(cols1[i]).innerHTML = '<span style="color: red; font-size: 14pt; margin-left: 10%;">&#9650; </span>'+
			'<span id="' + 'mup-'+cols1[i]+ '" class="dataValue">' + numberWithCommas(current[cols1[i]].toFixed()) + '</span><br>';
		}
		else if(prev[cols1[i]] > current[cols1[i]]){
			document.getElementById(cols1[i]).innerHTML = '<span style="color: green; font-size: 14pt; margin-left: 10%;">&#9660; </span>'+
			'<span id="' + 'mup-'+cols1[i]+ '" class="dataValue">' + numberWithCommas(current[cols1[i]].toFixed()) + '</span><br>';
		}
	}
	var btns1 = ['SPDC','TEPNG','NAOC'];
	$('#openUpstream').on('click',function(){
		var thisTable = '<div style="margin-left: 30px;"><table border="1" style="text-align: center;"><tr>'+
			'<th style="width: 100px;"></th><th style="width: 100px;">CO2 (L)</th><th style="width: 100px;">N2O (Kg)</th>'+
			'<th style="width: 100px;">CH4 (Kg)</th><th></th><th style="width: 100px;">CO2e (T)</th></tr><tr><th>Upstream</th>'+
			'<td id="' + 'mup-'+'SPDC_CO2'+ '" class="dataValue">'+numberWithCommas(current['SPDC_CO2'].toFixed())+'</td>'+
			'<td id="' + 'mup-'+'SPDC_N2O'+ '" class="dataValue">'+numberWithCommas(current['SPDC_N2O'].toFixed(2)*1000)+'</td>'+
			'<td id="' + 'mup-'+'SPDC_CH4'+ '" class="dataValue">'+numberWithCommas(current['SPDC_CH4'].toFixed(2)*1000)+'</td><td></td>'+
			'<td id="' + 'mup-'+'SPDC_CO2e'+ '" class="dataValue">'+numberWithCommas(current['SPDC_CO2e'].toFixed())+'</td></tr>'+
			'<tr><th>GTS</th><td>-</td><td>-</td>'+
			'<td id="' + 'mup-'+'SPDC_GTS_CH4'+ '" class="dataValue">'+numberWithCommas(current['SPDC_GTS_CH4'].toFixed(2))+'</td><td></td>'+
			'<td id="' + 'mup-'+'SPDC_GTS_CO2e'+ '" class="dataValue">'+numberWithCommas(current['SPDC_GTS_CO2e'].toFixed(2))+'</td></tr>'+
			'</table></div>';
		document.getElementById('upstreamDetails').innerHTML = thisTable;
		document.getElementById('id01').style.display='block';
		for(var i = 0;i<btns1.length;i++){
			document. getElementById(btns1[i]).className = "innerBtns"; 
		}
		document.getElementById('SPDC').className = "innerBtnSel";
	});
	$(document.body).click(function(evt){
        var clicked = evt.target;
        var currentID = clicked.id || "No ID!";
        for(var i = 0;i<btns1.length;i++){
            if (currentID != "No ID!" && btns1[i] == currentID){
                var thisTable = '<div style="margin-left: 30px;"><table border="1" style="text-align: center;"><tr>'+
					'<th style="width: 100px;"></th><th style="width: 100px;">CO2 (L)</th><th style="width: 100px;">N2O (Kg)</th>'+
					'<th style="width: 100px;">CH4 (Kg)</th><th></th><th style="width: 100px;">CO2e (T)</th></tr><tr><th>Upstream</th>'+
					'<td id="' + 'mup-'+currentID+'_CO2'+ '" class="dataValue">'+numberWithCommas(current[currentID+'_CO2'].toFixed())+'</td>'+
					'<td id="' + 'mup-'+currentID+'_N2O'+ '" class="dataValue">'+numberWithCommas(current[currentID+'_N2O'].toFixed(2)*1000)+'</td>'+
					'<td id="' + 'mup-'+currentID+'_CH4'+ '" class="dataValue">'+numberWithCommas(current[currentID+'_CH4'].toFixed(2)*1000)+'</td><td></td>'+
					'<td id="' + 'mup-'+currentID+'_CO2e'+ '" class="dataValue">'+numberWithCommas(current[currentID+'_CO2e'].toFixed())+'</td></tr>'+
					'<tr><th>GTS</th><td>-</td><td>-</td>'+
					'<td id="' + 'mup-'+currentID+'_GTS_CH4'+ '" class="dataValue">'+numberWithCommas(current[currentID+'_GTS_CH4'].toFixed(2))+'</td><td></td>'+
					'<td id="' + 'mup-'+currentID+'_GTS_CO2e'+ '" class="dataValue">'+numberWithCommas(current[currentID+'_GTS_CO2e'].toFixed(2))+'</td></tr>'+
					'</table></div>';
				document.getElementById('upstreamDetails').innerHTML = thisTable;
				for(var i = 0;i<btns1.length;i++){
					document. getElementById(btns1[i]).className = "innerBtns"; 
				}
				document.getElementById(currentID).className = "innerBtnSel";
                break;
            }
        }
    });
	$(document.body).click(function(evt){
        var clicked = evt.target;
        var currentID = clicked.id || "No ID!";
        if (currentID != "No ID!" && document.getElementById(currentID).className == 'dataValue' && currentID.split('-')[0] == 'mup'){
			current["ID"] = currentID;
			var thisID = currentID.split('-')[1];
			var selMths = []
			var selData = []
			selMths.push('x')
			selData.push(thisID);

			for(var i = 0;i<12;i++){
				var thisDate = addMonths(new Date(), i-12);
				var currDate = formatDate(thisDate)
				selMths.push(currDate);
				for(var j = 0;j<current["upstreamData"].length;j++){
					RecordDate = new Date(current["upstreamData"][j].RecordDate)
					if(RecordDate.getMonth() == thisDate.getMonth() && RecordDate.getFullYear() == thisDate.getFullYear()){
						var currData = (parseFloat(current["upstreamData"][j][thisID])).toFixed(2);
						break;
					}
					else{
						var currData = 'null';
					}
				}
				selData.push(currData);
			}
			document.getElementById('startDate').value = selMths[1];
			document.getElementById('endDate').value = selMths[selMths.length-1];
			var chart = bb.generate({
				size: {
					height: 250,
					width: 560
				},
				data: {
					x: "x",
					columns: [
						selMths,
						selData
					],
					type: "line",
				},
				axis: {
					x: {
						type: "timeseries",
						tick: {
							format: function(x) {
								return months(x.getMonth()) + '-' + (String(x.getFullYear())).slice(-2);
							}
						}
					},
				},
				bindto: "#lineChart"
			});
			document.getElementById('id02').style.display='block';
		}	
	});
}

function processShipData(data){
	current["shipData"] = sort_by_key(data, 'RecordDate');

	//Get list of data columns
	var colKeys = [];
	for(var i = 0;i<data.length;i++){
		Object.keys(data[i]).forEach(function(key){
			if(colKeys.indexOf(key) == -1){
				colKeys.push(key);
			}
		});
	}

	let uniqDates = data.filter(
        (myTable, index) => index === data.findIndex(
            other => myTable.RecordDate === other.RecordDate
        )
    );

    current["uniqDates"] = sort_by_key(uniqDates, 'RecordDate');

	var thisdata = {};
    var thisArray = [];
	var id = 0;

	var fltMgrs = ['NSML','BW_Gas','NYK','NMM','Temile','Marine_Partners'];

	for(var k = 0;k<current["uniqDates"].length;k++){
		RecordDate = current["uniqDates"][k].RecordDate;
		id = id+1
		thisdata.id = id;
		thisdata.RecordDate = RecordDate;
		for(var i = 0;i<fltMgrs.length;i++){
			Fleet_Mgr = fltMgrs[i];
			var CO2 = 0;
			var N2O = 0;
			var CH4 = 0;
			var CO2e = 0;
			var CII = 0;
			for(var j = 0;j<current["shipData"].length;j++){
				if(Fleet_Mgr == current["shipData"][j].Fleet_Mgr && RecordDate == current["shipData"][j].RecordDate){
					CO2 = CO2 + parseFloat(current["shipData"][j].CO2)
					N2O = N2O + parseFloat(current["shipData"][j].N2O)
					CH4 = CH4 + parseFloat(current["shipData"][j].CH4)
					CO2e = CO2e + parseFloat(current["shipData"][j].CO2e)
					CII = CII + parseFloat(current["shipData"][j].CII)
				}
			}
			thisdata[Fleet_Mgr+'_CO2'] = CO2;
			thisdata[Fleet_Mgr+'_N2O'] = N2O;
			thisdata[Fleet_Mgr+'_CH4'] = CH4;
			thisdata[Fleet_Mgr+'_CO2e'] = CO2e;
			thisdata[Fleet_Mgr+'_CII'] = CII;
		}
		thisArray.push({...thisdata})
	}
	current["fleetData"] = thisArray;

	var dd = new Date(data[current["shipData"].length-1]['RecordDate'])
	document.getElementById('shipDate').innerHTML = '[ ' + current["months"][dd.getMonth()] + '-' + dd.getFullYear() + ' ]';

	var colKeys2 = [];
	for(var i = 0;i<current["fleetData"].length;i++){
		Object.keys(current["fleetData"][i]).forEach(function(key){
			if(colKeys2.indexOf(key) == -1){
				colKeys2.push(key);
			}
		});
	}

	for(var i = 0;i<colKeys2.length-2;i++){
		current[colKeys2[i+2]] = parseFloat(current["fleetData"][current["fleetData"].length-1][colKeys2[i+2]]);
	}

	for(var i = 0;i<colKeys2.length-2;i++){
		prev[colKeys2[i+2]] = parseFloat(current["fleetData"][current["fleetData"].length-2][colKeys2[i+2]]);
	}

	//parse Shipping data to dashboard
	var cols1 = ['NSML_CO2e','BW_Gas_CO2e','NYK_CO2e','NMM_CO2e','Temile_CO2e','Marine_Partners_CO2e'];
	for(var i = 0;i<cols1.length;i++){
		if(prev[cols1[i]] == current[cols1[i]]){
			document.getElementById(cols1[i]).innerHTML = '<span style="color: orange; font-size: 14pt; margin-left: 0%;">&#9673; </span>'+
			'<span id="' + 'mfl-'+cols1[i]+ '" class="dataValue">' + numberWithCommas(current[cols1[i]].toFixed()) + '</span><br>';
		}
		else if(prev[cols1[i]] < current[cols1[i]]){
			document.getElementById(cols1[i]).innerHTML = '<span style="color: red; font-size: 14pt; margin-left: 0%;">&#9650; </span>'+
			'<span id="' + 'mfl-'+cols1[i]+ '" class="dataValue">' + numberWithCommas(current[cols1[i]].toFixed()) + '</span><br>';
		}
		else if(prev[cols1[i]] > current[cols1[i]]){
			document.getElementById(cols1[i]).innerHTML = '<span style="color: green; font-size: 14pt; margin-left: 0%;">&#9660; </span>'+
			'<span id="' + 'mfl-'+cols1[i]+ '" class="dataValue">' + numberWithCommas(current[cols1[i]].toFixed()) + '</span><br>';
		}
	}

	$('#openShip').on('click',function(){
		var thisTable = '<div style="margin-left: 30px;"><table border="1" style="text-align: center;"><tr>'+
			'<th style="width: 140px;">Vessel</th><th style="width: 80px;">CO2 (T)</th><th style="width: 80px;">N2O (Kg)</th>'+
			'<th style="width: 80px;">CH4 (Kg)</th><th></th><th style="width: 80px;">CO2e (T)</th><th style="width: 80px;">CII</th></tr><tr>';
		for(var j = 0;j<current["shipData"].length;j++){
			if(current["shipData"][j].Fleet_Mgr == 'NSML' && current["shipData"][j].RecordDate == current["shipData"][current["shipData"].length-1].RecordDate){
				thisTable = thisTable + '<th style="text-align: left;">' + current["shipData"][j].Vessel_Name + '</th>'+
				'<td class="dataValue">'+numberWithCommas((parseFloat(current["shipData"][j].CO2)).toFixed())+'</td>'+
				'<td class="dataValue">'+numberWithCommas((parseFloat(current["shipData"][j].N2O)*1000).toFixed())+'</td>'+
				'<td class="dataValue">'+numberWithCommas((parseFloat(current["shipData"][j].CH4)*1000).toFixed())+'</td><td></td>'+
				'<td class="dataValue">'+numberWithCommas((parseFloat(current["shipData"][j].CO2e)).toFixed())+'</td>'+
				'<td class="dataValue">'+numberWithCommas((parseFloat(current["shipData"][j].CII)).toFixed(2))+'</td></tr>'
			}
		}
		thisTable = thisTable + '</table></div>';
		document.getElementById('shipDetails').innerHTML = thisTable;
		document.getElementById('id04').style.display='block';
		for(var i = 0;i<fltMgrs.length;i++){
			document. getElementById(fltMgrs[i]).className = "innerBtns"; 
		}
		document.getElementById('NSML').className = "innerBtnSel";
	});
	$(document.body).click(function(evt){
        var clicked = evt.target;
        var currentID = clicked.id || "No ID!";
        for(var i = 0;i<fltMgrs.length;i++){
            if (currentID != "No ID!" && fltMgrs[i] == currentID){
                var thisTable = '<div style="margin-left: 30px;"><table border="1" style="text-align: center;"><tr>'+
					'<th style="width: 140px;">Vessel</th><th style="width: 80px;">CO2 (T)</th><th style="width: 80px;">N2O (Kg)</th>'+
					'<th style="width: 80px;">CH4 (Kg)</th><th></th><th style="width: 80px;">CO2e (T)</th><th style="width: 80px;">CII</th></tr><tr>';
				for(var j = 0;j<current["shipData"].length;j++){
					if(current["shipData"][j].Fleet_Mgr == currentID && current["shipData"][j].RecordDate == current["shipData"][current["shipData"].length-1].RecordDate){
						thisTable = thisTable + '<th style="text-align: left;">' + current["shipData"][j].Vessel_Name + '</th>'+
						'<td class="dataValue">'+numberWithCommas((parseFloat(current["shipData"][j].CO2)).toFixed())+'</td>'+
						'<td class="dataValue">'+numberWithCommas((parseFloat(current["shipData"][j].N2O)*1000).toFixed())+'</td>'+
						'<td class="dataValue">'+numberWithCommas((parseFloat(current["shipData"][j].CH4)*1000).toFixed())+'</td><td></td>'+
						'<td class="dataValue">'+numberWithCommas((parseFloat(current["shipData"][j].CO2e)).toFixed())+'</td>'+
						'<td class="dataValue">'+numberWithCommas((parseFloat(current["shipData"][j].CII)).toFixed(2))+'</td></tr>'
					}
				}
				thisTable = thisTable + '</table></div>';
				document.getElementById('shipDetails').innerHTML = thisTable;
				for(var i = 0;i<fltMgrs.length;i++){
					document. getElementById(fltMgrs[i]).className = "innerBtns"; 
				}
				document.getElementById(currentID).className = "innerBtnSel";
                break;
            }
        }
    });
	$(document.body).click(function(evt){
        var clicked = evt.target;
        var currentID = clicked.id || "No ID!";
        if (currentID != "No ID!" && document.getElementById(currentID).className == 'dataValue' && currentID.split('-')[0] == 'mfl'){
			current["ID"] = currentID;
			var thisID = currentID.split('-')[1];
			var selMths = []
			var selData = []
			selMths.push('x')
			selData.push(thisID);

			for(var i = 0;i<12;i++){
				var thisDate = addMonths(new Date(), i-12);
				var currDate = formatDate(thisDate)
				selMths.push(currDate);
				for(var j = 0;j<current["fleetData"].length;j++){
					RecordDate = new Date(current["fleetData"][j].RecordDate)
					if(RecordDate.getMonth() == thisDate.getMonth() && RecordDate.getFullYear() == thisDate.getFullYear()){
						var currData = (parseFloat(current["fleetData"][j][thisID])).toFixed(2);
						break;
					}
					else{
						var currData = 'null';
					}
				}
				selData.push(currData);
			}
			document.getElementById('startDate').value = selMths[1];
			document.getElementById('endDate').value = selMths[selMths.length-1];
			var chart = bb.generate({
				size: {
					height: 250,
					width: 560
				},
				data: {
					x: "x",
					columns: [
						selMths,
						selData
					],
					type: "line",
				},
				axis: {
					x: {
						type: "timeseries",
						tick: {
							format: function(x) {
								return months(x.getMonth()) + '-' + (String(x.getFullYear())).slice(-2);
							}
						}
					},
				},
				bindto: "#lineChart"
			});
			document.getElementById('id02').style.display='block';
		}	
	});
}

$('#createChart').on('click',function(){
	var startDate = new Date(document.getElementById('startDate').value);
	var sm = new Date(startDate.setDate(startDate.getDate()+1));
	var sd = new Date(startDate.setDate(startDate.getDate()-1));
	var startMth = addMonths(sm, -1);
	var endDate = new Date(document.getElementById('endDate').value)
	var diffTime = Math.abs(endDate - startDate);
	var diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
	var diffWks = (diffDays/7).toFixed()
	var diffMths = endDate.getMonth() - sd.getMonth() + (12 * (endDate.getFullYear() - sd.getFullYear()));

	if(startDate > endDate){
		alert("Start date cannot be after the End date!")
	}
	else if (current["ID"] != "No ID!" && document.getElementById(current["ID"]).className == 'dataValue' && current["ID"].split('-')[0] == 'd'){
		var thisID = current["ID"].split('-')[1];
		var selMths = []
		var selData = []
		selMths.push('x')
		selData.push(thisID);

		for(var i = 0;i<diffDays;i++){
			var d = sd;
			var thisDate = new Date(d.setDate(d.getDate()+1));
			var currDate = formatDate(thisDate)
			selMths.push(currDate)
			for(var j = 0;j<current["piDailyData"].length;j++){
				RecordDate = new Date(current["piDailyData"][j].RecordDate)
				if(RecordDate.getDate() == thisDate.getDate() && RecordDate.getMonth() == thisDate.getMonth() && RecordDate.getFullYear() == thisDate.getFullYear()){
					var currData = (parseFloat(current["piDailyData"][j][thisID])).toFixed(2);
					break;
				}
				else{
					var currData = 'null';
				}
			}
			selData.push(currData);
		}
		document.getElementById('startDate').value = selMths[1];
		document.getElementById('endDate').value = selMths[selMths.length-1];
		var chart = bb.generate({
			size: {
				height: 250,
				width: 560
			},
			data: {
				x: "x",
				columns: [
					selMths,
					selData
				],
				type: "line", // for ESM specify as: line()
			},
			axis: {
				x: {
					type: "timeseries",
					tick: {
						format: function(x) {
							return x.getDate() + '-' + months(x.getMonth()) + '-' + (String(x.getFullYear())).slice(-2);
						}
					},
				},
				y: {
					tick: {
						culling: {
						  max: 4
						}
					}
				}
			},
			point: {
				pattern: [
				  "<circle cx='0' cy='0' r='0'></circle>"
				]
			},
			zoom: {
				enabled: true,
				type: "drag"
			},
			bindto: "#lineChart"
		});
		document.getElementById('id02').style.display='block';
	}
	else if (current["ID"] != "No ID!" && document.getElementById(current["ID"]).className == 'dataValue' && current["ID"].split('-')[0] == 'wpl'){
		var thisID = current["ID"].split('-')[1];
		var selMths = []
		var selData = []
		selMths.push('x')
		selData.push(thisID);
		//sd = new Date(sd.setDate(sd.getDate()-7));
		for(var j = 0;j<current["flareData"].length;j++){
			RecordDate = new Date(current["flareData"][j].RecordDate)
			if(weekNumber(RecordDate) == weekNumber(sd) && RecordDate.getFullYear() == sd.getFullYear()){
				var currData = (parseFloat(current["flareData"][j][thisID])).toFixed(2);
				break;
			}
			else{
				var currData = 'null';
			}
		}
		selMths.push(formatDate(sd));
		selData.push(currData);

		for(var i = 0;i<diffWks;i++){
			var d = sd;
			var thisDate = new Date(d.setDate(d.getDate()+7));
			var currDate = formatDate(thisDate)
			for(var j = 0;j<current["flareData"].length;j++){
				RecordDate = new Date(current["flareData"][j].RecordDate)
				if(weekNumber(RecordDate) == weekNumber(thisDate) && RecordDate.getFullYear() == thisDate.getFullYear()){
					var currData = (parseFloat(current["flareData"][j][thisID])).toFixed(2);
					break;
				}
				else{
					var currData = 'null';
				}
			}
			selMths.push(currDate);
			selData.push(currData);
		}
		document.getElementById('startDate').value = selMths[1];
		document.getElementById('endDate').value = selMths[selMths.length-1];
		var chart = bb.generate({
			size: {
				height: 250,
				width: 560
			},
			data: {
				x: "x",
				columns: [
					selMths,
					selData
				],
				type: "line", // for ESM specify as: line()
			},
			axis: {
				x: {
					type: "timeseries",
					tick: {
						format: function(x) {
							return 'Wk ' + weekNumber(x) + ', ' + (String(x.getFullYear())).slice(-2);
						}
					},
				},
				y: {
					tick: {
						culling: {
						  max: 4
						}
					}
				}
			},
			point: {
				pattern: [
				  "<circle cx='0' cy='0' r='0'></circle>"
				]
			},
			zoom: {
				enabled: true,
				type: "drag"
			},
			bindto: "#lineChart"
		});
		document.getElementById('id02').style.display='block';
	}
	else if (current["ID"] != "No ID!" && document.getElementById(current["ID"]).className == 'dataValue' && current["ID"].split('-')[0] == 'mpl'){
		var thisID = current["ID"].split('-')[1];
		var selMths = []
		var selData = []
		selMths.push('x')
		selData.push(thisID);

		for(var i = 0;i<diffMths+1;i++){
			var thisDate = addMonths(startMth, 1);
			var currDate = formatDate(thisDate)
			selMths.push(currDate);
			for(var j = 0;j<current["plantMonthlyData"].length;j++){
				RecordDate = new Date(current["plantMonthlyData"][j].RecordDate)
				if(RecordDate.getMonth() == thisDate.getMonth() && RecordDate.getFullYear() == thisDate.getFullYear()){
					var currData = (parseFloat(current["plantMonthlyData"][j][thisID])).toFixed(2);
					break;
				}
				else{
					var currData = 'null';
				}
			}
			selData.push(currData);
		}
		var chart = bb.generate({
			size: {
				height: 250,
				width: 560
			},
			data: {
				x: "x",
				columns: [
					selMths,
					selData
				],
				type: "line",
			},
			axis: {
				x: {
					type: "timeseries",
					tick: {
						format: function(x) {
							return months(x.getMonth()) + '-' + (String(x.getFullYear())).slice(-2);
						}
					}
				},
			},
			point: {
				pattern: [
				  "<circle cx='0' cy='0' r='0'></circle>"
				]
			},
			bindto: "#lineChart"
		});
		document.getElementById('id02').style.display='block';
	}
	else if (current["ID"] != "No ID!" && document.getElementById(current["ID"]).className == 'dataValue' && current["ID"].split('-')[0] == 'moff'){
		var thisID = current["ID"].split('-')[1];
		var selMths = []
		var selData = []
		selMths.push('x')
		selData.push(thisID);

		for(var i = 0;i<diffMths+1;i++){
			var thisDate = addMonths(startMth, 1);
			var currDate = formatDate(thisDate)
			selMths.push(currDate);
			for(var j = 0;j<current["officeData"].length;j++){
				RecordDate = new Date(current["officeData"][j].RecordDate)
				if(RecordDate.getMonth() == thisDate.getMonth() && RecordDate.getFullYear() == thisDate.getFullYear()){
					var currData = (parseFloat(current["officeData"][j][thisID])).toFixed(2);
					break;
				}
				else{
					var currData = 'null';
				}
			}
			selData.push(currData);
		}
		var chart = bb.generate({
			size: {
				height: 250,
				width: 560
			},
			data: {
				x: "x",
				columns: [
					selMths,
					selData
				],
				type: "line",
			},
			axis: {
				x: {
					type: "timeseries",
					tick: {
						format: function(x) {
							return months(x.getMonth()) + '-' + (String(x.getFullYear())).slice(-2);
						}
					}
				},
			},
			point: {
				pattern: [
				  "<circle cx='0' cy='0' r='0'></circle>"
				]
			},
			bindto: "#lineChart"
		});
		document.getElementById('id02').style.display='block';
	}
	else if (current["ID"] != "No ID!" && document.getElementById(current["ID"]).className == 'dataValue' && current["ID"].split('-')[0] == 'mof'){
		var thisID = current["ID"].split('-')[1];
		var selMths = []
		var selData = []
		selMths.push('x')
		selData.push(thisID);

		for(var i = 0;i<diffMths+1;i++){
			var thisDate = addMonths(startMth, 1);
			var currDate = formatDate(thisDate)
			selMths.push(currDate);
			for(var j = 0;j<current["offData"].length;j++){
				RecordDate = new Date(current["offData"][j].RecordDate)
				if(RecordDate.getMonth() == thisDate.getMonth() && RecordDate.getFullYear() == thisDate.getFullYear()){
					var currData = (parseFloat(current["offData"][j][thisID])).toFixed(2);
					break;
				}
				else{
					var currData = 'null';
				}
			}
			selData.push(currData);
		}
		var chart = bb.generate({
			size: {
				height: 250,
				width: 560
			},
			data: {
				x: "x",
				columns: [
					selMths,
					selData
				],
				type: "line",
			},
			axis: {
				x: {
					type: "timeseries",
					tick: {
						format: function(x) {
							return months(x.getMonth()) + '-' + (String(x.getFullYear())).slice(-2);
						}
					}
				},
			},
			point: {
				pattern: [
				  "<circle cx='0' cy='0' r='0'></circle>"
				]
			},
			bindto: "#lineChart"
		});
		document.getElementById('id02').style.display='block';
	}
	else if (current["ID"] != "No ID!" && document.getElementById(current["ID"]).className == 'dataValue' && current["ID"].split('-')[0] == 'mlss'){
		var thisID = current["ID"].split('-')[1];
		var selMths = []
		var selData = []
		selMths.push('x')
		selData.push(thisID);

		for(var i = 0;i<diffMths+1;i++){
			var thisDate = addMonths(startMth, 1);
			var currDate = formatDate(thisDate)
			selMths.push(currDate);
			for(var j = 0;j<current["logisticsData"].length;j++){
				RecordDate = new Date(current["logisticsData"][j].RecordDate)
				if(RecordDate.getMonth() == thisDate.getMonth() && RecordDate.getFullYear() == thisDate.getFullYear()){
					var currData = (parseFloat(current["logisticsData"][j][thisID])).toFixed(2);
					break;
				}
				else{
					var currData = 'null';
				}
			}
			selData.push(currData);
		}
		var chart = bb.generate({
			size: {
				height: 250,
				width: 560
			},
			data: {
				x: "x",
				columns: [
					selMths,
					selData
				],
				type: "line",
			},
			axis: {
				x: {
					type: "timeseries",
					tick: {
						format: function(x) {
							return months(x.getMonth()) + '-' + (String(x.getFullYear())).slice(-2);
						}
					}
				},
			},
			point: {
				pattern: [
				  "<circle cx='0' cy='0' r='0'></circle>"
				]
			},
			bindto: "#lineChart"
		});
		document.getElementById('id02').style.display='block';
	}
	else if (current["ID"] != "No ID!" && document.getElementById(current["ID"]).className == 'dataValue' && current["ID"].split('-')[0] == 'mp'){
		var thisID = current["ID"].split('-')[1];
		var selMths = []
		var selData = []
		selMths.push('x')
		selData.push(thisID);

		for(var i = 0;i<diffMths+1;i++){
			var thisDate = addMonths(startMth, 1);
			var currDate = formatDate(thisDate)
			selMths.push(currDate);
			for(var j = 0;j<current["projectsData"].length;j++){
				RecordDate = new Date(current["projectsData"][j].RecordDate)
				if(RecordDate.getMonth() == thisDate.getMonth() && RecordDate.getFullYear() == thisDate.getFullYear()){
					var currData = (parseFloat(current["projectsData"][j][thisID])).toFixed(2);
					break;
				}
				else{
					var currData = 'null';
				}
			}
			selData.push(currData);
		}
		var chart = bb.generate({
			size: {
				height: 250,
				width: 560
			},
			data: {
				x: "x",
				columns: [
					selMths,
					selData
				],
				type: "line",
			},
			axis: {
				x: {
					type: "timeseries",
					tick: {
						format: function(x) {
							return months(x.getMonth()) + '-' + (String(x.getFullYear())).slice(-2);
						}
					}
				},
			},
			point: {
				pattern: [
				  "<circle cx='0' cy='0' r='0'></circle>"
				]
			},
			bindto: "#lineChart"
		});
		document.getElementById('id02').style.display='block';
	}
	else if (current["ID"] != "No ID!" && document.getElementById(current["ID"]).className == 'dataValue' && current["ID"].split('-')[0] == 'mpr'){
		var thisID = current["ID"].split('-')[1];
		var selMths = []
		var selData = []
		selMths.push('x')
		selData.push(thisID);

		for(var i = 0;i<diffMths+1;i++){
			var thisDate = addMonths(startMth, 1);
			var currDate = formatDate(thisDate)
			selMths.push(currDate);
			for(var j = 0;j<current["projData"].length;j++){
				RecordDate = new Date(current["projData"][j].RecordDate)
				if(RecordDate.getMonth() == thisDate.getMonth() && RecordDate.getFullYear() == thisDate.getFullYear()){
					var currData = (parseFloat(current["projData"][j][thisID])).toFixed(2);
					break;
				}
				else{
					var currData = 'null';
				}
			}
			selData.push(currData);
		}
		var chart = bb.generate({
			size: {
				height: 250,
				width: 560
			},
			data: {
				x: "x",
				columns: [
					selMths,
					selData
				],
				type: "line",
			},
			axis: {
				x: {
					type: "timeseries",
					tick: {
						format: function(x) {
							return months(x.getMonth()) + '-' + (String(x.getFullYear())).slice(-2);
						}
					}
				},
			},
			point: {
				pattern: [
				  "<circle cx='0' cy='0' r='0'></circle>"
				]
			},
			bindto: "#lineChart"
		});
		document.getElementById('id02').style.display='block';
	}
	else if (current["ID"] != "No ID!" && document.getElementById(current["ID"]).className == 'dataValue' && current["ID"].split('-')[0] == 'mup'){
		var thisID = current["ID"].split('-')[1];
		var selMths = []
		var selData = []
		selMths.push('x')
		selData.push(thisID);

		for(var i = 0;i<diffMths+1;i++){
			var thisDate = addMonths(startMth, 1);
			var currDate = formatDate(thisDate)
			selMths.push(currDate);
			for(var j = 0;j<current["upstreamData"].length;j++){
				RecordDate = new Date(current["upstreamData"][j].RecordDate)
				if(RecordDate.getMonth() == thisDate.getMonth() && RecordDate.getFullYear() == thisDate.getFullYear()){
					var currData = (parseFloat(current["upstreamData"][j][thisID])).toFixed(2);
					break;
				}
				else{
					var currData = 'null';
				}
			}
			selData.push(currData);
		}
		var chart = bb.generate({
			size: {
				height: 250,
				width: 560
			},
			data: {
				x: "x",
				columns: [
					selMths,
					selData
				],
				type: "line",
			},
			axis: {
				x: {
					type: "timeseries",
					tick: {
						format: function(x) {
							return months(x.getMonth()) + '-' + (String(x.getFullYear())).slice(-2);
						}
					}
				},
			},
			point: {
				pattern: [
				  "<circle cx='0' cy='0' r='0'></circle>"
				]
			},
			bindto: "#lineChart"
		});
		document.getElementById('id02').style.display='block';
	}
	else if (current["ID"] != "No ID!" && document.getElementById(current["ID"]).className == 'dataValue' && current["ID"].split('-')[0] == 'mfl'){
		var thisID = current["ID"].split('-')[1];
		var selMths = []
		var selData = []
		selMths.push('x')
		selData.push(thisID);

		for(var i = 0;i<diffMths+1;i++){
			var thisDate = addMonths(startMth, 1);
			var currDate = formatDate(thisDate)
			selMths.push(currDate);
			for(var j = 0;j<current["fleetData"].length;j++){
				RecordDate = new Date(current["fleetData"][j].RecordDate)
				if(RecordDate.getMonth() == thisDate.getMonth() && RecordDate.getFullYear() == thisDate.getFullYear()){
					var currData = (parseFloat(current["fleetData"][j][thisID])).toFixed(2);
					break;
				}
				else{
					var currData = 'null';
				}
			}
			selData.push(currData);
		}
		var chart = bb.generate({
			size: {
				height: 250,
				width: 560
			},
			data: {
				x: "x",
				columns: [
					selMths,
					selData
				],
				type: "line",
			},
			axis: {
				x: {
					type: "timeseries",
					tick: {
						format: function(x) {
							return months(x.getMonth()) + '-' + (String(x.getFullYear())).slice(-2);
						}
					}
				},
			},
			point: {
				pattern: [
				  "<circle cx='0' cy='0' r='0'></circle>"
				]
			},
			bindto: "#lineChart"
		});
		document.getElementById('id02').style.display='block';
	}
	else if (current["ID"] != "No ID!" && document.getElementById(current["ID"]).className == 'dataValue2' && current["ID"].split('-')[0] == 'msumm'){
		var thisID = current["ID"].split('-')[1];
		var selMths = []
		var selData = []
		selMths.push('x')
		selData.push(thisID);

		for(var i = 0;i<diffMths+1;i++){
			var thisDate = addMonths(startMth, 1);
			var currDate = formatDate(thisDate)
			selMths.push(currDate);
			for(var j = 0;j<current["summaryData"].length;j++){
				RecordDate = new Date(current["summaryData"][j].RecordDate)
				if(RecordDate.getMonth() == thisDate.getMonth() && RecordDate.getFullYear() == thisDate.getFullYear()){
					var currData = (parseFloat(current["summaryData"][j][thisID])).toFixed(2);
					break;
				}
				else{
					var currData = 'null';
				}
			}
			selData.push(currData);
		}
		var chart = bb.generate({
			size: {
				height: 250,
				width: 560
			},
			data: {
				x: "x",
				columns: [
					selMths,
					selData
				],
				type: "line",
			},
			axis: {
				x: {
					type: "timeseries",
					tick: {
						format: function(x) {
							return months(x.getMonth()) + '-' + (String(x.getFullYear())).slice(-2);
						}
					}
				},
			},
			point: {
				pattern: [
				  "<circle cx='0' cy='0' r='0'></circle>"
				]
			},
			bindto: "#lineChart"
		});
		document.getElementById('id02').style.display='block';
	}
});

current["months"] = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];

function months(i){
	if(i==0){return "Jan";}
	else if(i==1){return "Feb";}
	else if(i==2){return "Mar";}
	else if(i==3){return "Apr";}
	else if(i==4){return "May";}
	else if(i==5){return "Jun";}
	else if(i==6){return "Jul";}
	else if(i==7){return "Aug";}
	else if(i==8){return "Sep";}
	else if(i==9){return "Oct";}
	else if(i==10){return "Nov";}
	else if(i==11){return "Dec";}
	else{return i}		
}

function sort_by_key(array, key){
	return array.sort(function(a, b){
		var x = a[key]; var y = b[key];
		return ((x < y) ? -1 : ((x > y) ? 1 : 0));
	});
}

function formatDate(date) {
    var d = new Date(date),
        month = '' + (d.getMonth() + 1),
        day = '' + d.getDate(),
        year = d.getFullYear();

    if (month.length < 2) month = '0' + month;
    if (day.length < 2) day = '0' + day;

    return [year, month, day].join('-');
}

function addMonths(date, months) {
	date.setMonth(date.getMonth() + months);
	return date;
  }

$('#showDetails').on('click',function(){
	document.getElementById('homepage').style.display='block';
	document.getElementById('summary').style.display='none';
});

$('#showSummary').on('click',function(){
	document.getElementById('homepage').style.display='none';
	document.getElementById('summary').style.display='block';
});

function refreshPg(){
	var summMonth = document.getElementById('summMonth').value;
	var thisDate = new Date(summMonth + '-01');
	var cols1 = ['Upstream_CO2e','Plant_CO2e','Shipping_CO2e','Logistics_CO2e','Office_CO2e','Project_CO2e'];

	for(var j = 0;j<current["summaryData"].length;j++){
		var RecordDate = new Date(current["summaryData"][j]['RecordDate']);
		if(RecordDate.getMonth() == thisDate.getMonth() && RecordDate.getFullYear() == thisDate.getFullYear()){
			for(var i = 0;i<cols1.length;i++){
				if(current["summaryData"][j][cols1[i]] == 'nan'){
					current[cols1[i]] = '-';
					prev[cols1[i]] = '-';
				}
				else{
					current[cols1[i]] = (parseFloat(current["summaryData"][j][cols1[i]])).toFixed();
					prev[cols1[i]] = (parseFloat(current["summaryData"][j-1][cols1[i]])).toFixed();
				}
				current[cols1[i]+'_Date'] = current["summaryData"][j]['RecordDate'];
			}
			break;
		}
		else{
			for(var i = 0;i<cols1.length;i++){
				current[cols1[i]] = '-';
				prev[cols1[i]] = '-';
				current[cols1[i]+'_Date'] = thisDate;
			}
		}
	}
	for(var i = 0;i<cols1.length;i++){
		var dd = new Date(current[cols1[i]+'_Date'])
		document.getElementById(cols1[i]+'_Date').innerHTML = '[ ' + current["months"][dd.getMonth()] + '-' + dd.getFullYear() + ' ]';
		if(current[cols1[i]] == '-'){
			document.getElementById(cols1[i]).innerHTML = '<span id="' + 'msumm-'+cols1[i]+ '" class="dataValue2">' + numberWithCommas(current[cols1[i]]) + '</span>';
		}
		else if(prev[cols1[i]] == current[cols1[i]]){
			document.getElementById(cols1[i]).innerHTML = '<span style="color: orange; font-size: 30pt; margin-left: 0%;">&#9673; </span>'+
			'<span id="' + 'msumm-'+cols1[i]+ '" class="dataValue2">' + numberWithCommas(current[cols1[i]]) + '</span>';
		}
		else if(prev[cols1[i]] < current[cols1[i]]){
			document.getElementById(cols1[i]).innerHTML = '<span style="color: red; font-size: 30pt; margin-left: 0%;">&#9650; </span>'+
			'<span id="' + 'msumm-'+cols1[i]+ '" class="dataValue2">' + numberWithCommas(current[cols1[i]]) + '</span>';
		}
		else if(prev[cols1[i]] > current[cols1[i]]){
			document.getElementById(cols1[i]).innerHTML = '<span style="color: green; font-size: 30pt; margin-left: 0%;">&#9660; </span>'+
			'<span id="' + 'msumm-'+cols1[i]+ '" class="dataValue2">' + numberWithCommas(current[cols1[i]]) + '</span>';
		}
	}
}

function homeClick(){
	$.ajax({
		url : 'php/piDaily.php', // your php file
		type : 'GET', // type of the HTTP request
		cache: false,
		success : function(data){
			var obj = jQuery.parseJSON(data);
			processPiDaily(obj);
			$.ajax({
				url : 'php/flareData.php', // your php file
				type : 'GET', // type of the HTTP request
				cache: false,
				success : function(data){
					var obj = jQuery.parseJSON(data);
					processFlareData(obj);
					$.ajax({
						url : 'php/plantMonthly.php', // your php file
						type : 'GET', // type of the HTTP request
						cache: false,
						success : function(data){
							var obj = jQuery.parseJSON(data);
							processPlantMonthly(obj);
							$.ajax({
								url : 'php/getPILive.php', // your php file
								type : 'GET', // type of the HTTP request
								cache: false,
								success : function(data){
									var obj = jQuery.parseJSON(data);
									processLiveData(obj);
									$.ajax({
										url : 'php/officeData.php', // your php file
										type : 'GET', // type of the HTTP request
										cache: false,
										success : function(data){
											var obj = jQuery.parseJSON(data);
											processOffData(obj);
											$.ajax({
												url : 'php/getOfficesData.php', // your php file
												type : 'GET', // type of the HTTP request
												cache: false,
												success : function(data){
													var obj = jQuery.parseJSON(data);
													processOfficesData(obj);
													$.ajax({
														url : 'php/logisticsData.php', // your php file
														type : 'GET', // type of the HTTP request
														cache: false,
														success : function(data){
															var obj = jQuery.parseJSON(data);
															processLogisticsData(obj);
															$.ajax({
																url : 'php/upstreamData.php', // your php file
																type : 'GET', // type of the HTTP request
																cache: false,
																success : function(data){
																	var obj = jQuery.parseJSON(data);
																	processUpstreamData(obj);
																	$.ajax({
																		url : 'php/projData.php', // your php file
																		type : 'GET', // type of the HTTP request
																		cache: false,
																		success : function(data){
																			var obj = jQuery.parseJSON(data);
																			processprojData(obj);
																			$.ajax({
																				url : 'php/projectData.php', // your php file
																				type : 'GET', // type of the HTTP request
																				cache: false,
																				success : function(data){
																					var obj = jQuery.parseJSON(data);
																					processprojectData(obj);
																					$.ajax({
																						url : 'php/shipData.php', // your php file
																						type : 'GET', // type of the HTTP request
																						cache: false,
																						success : function(data){
																							var obj = jQuery.parseJSON(data);
																							processShipData(obj);
																							$.ajax({
																								url : 'php/summaryData.php', // your php file
																								type : 'GET', // type of the HTTP request
																								cache: false,
																								success : function(data){
																									var obj = jQuery.parseJSON(data);
																									processSummaryData(obj);
																								}
																							});
																						}
																					});
																				}
																			});
																		}
																	});
																}
															});
														}
													});
												}
											});
										}
									});
								}
							});
						}
					});
				}
			});
		}
	});
}
$('#toGrave').on('click',function(){
	document.getElementById('cradleGate').style.display='none';
	document.getElementById('cradleGrave').style.display='block';
});
$('#toGate').on('click',function(){
	document.getElementById('cradleGate').style.display='block';
	document.getElementById('cradleGrave').style.display='none';
});
$('#toGrave2').on('click',function(){
	document.getElementById('cradleGate2').style.display='none';
	document.getElementById('cradleGrave2').style.display='block';
});
$('#toGate2').on('click',function(){
	document.getElementById('cradleGate2').style.display='block';
	document.getElementById('cradleGrave2').style.display='none';
});

Number.prototype.padLeft = function(base,chr){
    var  len = (String(base || 10).length - String(this).length)+1;
    return len > 0? new Array(len).join(chr || '0')+this : this;
}
function convDate(d){
    var dformat = d.getDate().padLeft() + '/' + 
        (d.getMonth()+1).padLeft() + '/' +
        d.getFullYear() + ' ' +
        d.getHours().padLeft() + ':' +
        d.getMinutes().padLeft() + ':' +
        d.getSeconds().padLeft()

    return dformat;
}

onInactive(3 * 60 * 1000, function () {
    document.getElementById('id08').style.display='block';
	// Set the date we're counting down to
    current["countDownDate"] = new Date().getTime() + 2*60*1000;
});

function onInactive(ms, cb) {
    var wait = setTimeout(cb, ms);
	var wait2 = setTimeout(updateVisit, 4.85 * 60 * 1000);
    document.onclick = document.onmousemove = document.mousedown = document.mouseup = document.onkeydown = document.onkeyup = document.focus = function () {
        clearTimeout(wait);
		clearTimeout(wait2);
        wait = setTimeout(cb, ms);
		wait2 = setTimeout(updateVisit, 4.85 * 60 * 1000);
    };
}

//Posting Users Visit
function postVisit(){
	var postElement = "INSERT INTO Page_Visits VALUES(";
	var RecordDate2 = formatDate(new Date());
	var Logoff_Time = "";
	var Page_Link = "dashboards/mrv/index.html";
	var Comment = "";
	var UpdatedDate = formatDate(new Date());
	var UpdatedBy = "DataSys";
	currentt["Page_Visited"] = "MRV Dashboard";
	currentt["Logon_Time"] = convDate(new Date());
	
	postElement = postElement + "'" + RecordDate2 + "','" + UpdatedDate + "','" + UpdatedBy + "','" + currentt["Page_Visited"] + "','" + currentt["user"] + "','" + currentt["Logon_Time"] + "','" + Logoff_Time  + "','" + Comment + "','" + Page_Link + "')"
	postDb (postElement);
}
function updateVisit() {
	var postElement = "UPDATE Page_Visits SET Logoff_Time='" + convDate(new Date()) + "', Comment='Session timed out' WHERE Visitor='" + currentt["user"] + "' AND Logoff_Time='' AND Logon_Time='" + currentt["Logon_Time"] + "' AND Page_Visited='" + currentt["Page_Visited"] + "'";
	postDb (postElement);
	setTimeout(switchTab, 0.15*60*1000);
}
function switchTab() {
	//console.log("Switch Tab")
	location.replace("http://bny-s-t117/dashboards/mrv/timeout.html")
}
window.addEventListener('beforeunload', function (e) {
	var postElement = "UPDATE Page_Visits SET Logoff_Time='" + convDate(new Date()) + "', Comment='Page closed by user' WHERE Visitor='" + currentt["user"] + "' AND Logoff_Time='' AND Logon_Time='" + currentt["Logon_Time"] + "' AND Page_Visited='" + currentt["Page_Visited"] + "'";
	postDb (postElement);
});

// Update the count down every 1 second
var x = setInterval(function() {
  // Get today's date and time
  var now = new Date().getTime();    
  // Find the distance between now and the count down date
  var distance = current["countDownDate"] - now;    
  // Time calculations for days, hours, minutes and seconds
  var days = Math.floor(distance / (1000 * 60 * 60 * 24));
  var hours = Math.floor((distance % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60));
  var minutes = Math.floor((distance % (1000 * 60 * 60)) / (1000 * 60));
  var seconds = Math.floor((distance % (1000 * 60)) / 1000);    
  // Output the result in an element with id="timeLeft"
  document.getElementById("timeLeft").innerHTML = minutes + "m " + seconds + "s ";
}, 1000);