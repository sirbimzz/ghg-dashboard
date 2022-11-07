var currentt = null;
currentt = {};

$.ajax({
	url : 'php/getUser.php', // your php file
	type : 'GET', // type of the HTTP request
	//async: true,
	cache: false,
	success : function(data){
		var obj = data.split('\\');
		var userName = obj[1];
		processUser(userName);			
	}
});

function processUser(data){
	currentt["user"] = data;
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
																									postVisit();
																									if(window.screen.width > 300){
																										document.getElementById("summary").style.height = window.screen.height - 150 + 'px';
																										document.getElementById("summary").style.width = window.screen.width - 20 + 'px';
																										document.getElementById("homepage").style.height = window.screen.height - 150 + 'px';
																										document.getElementById("homepage").style.width = window.screen.width - 20 + 'px';
																									}
																									document.getElementById('homepage').style.display='none';
																									document.getElementById('summary').style.display='block';
																									document.getElementById('loading').style.display='none';
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


window.setInterval(function(){
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
}, 5 * 60 * 1000);