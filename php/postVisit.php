<?php 

include 'config/conn.php';

$RecordDate=$_POST['RecordDate'];
$Page_Visited=$_POST['Page_Visited'];
$Visitor=$_POST['Visitor'];
$Logon_Time=$_POST['Logon_Time'];
$Logoff_Time=$_POST['Logoff_Time'];
$Comment=$_POST['Comment'];
$UpdatedDate=$_POST['UpdatedDate'];
$UpdatedBy=$_POST['UpdatedBy'];

$sql = "INSERT INTO Page_Visits 
VALUES ('$RecordDate','$Page_Visited','$Visitor','$Logon_Time','$Logoff_Time','$Comment','$UpdatedDate','$UpdatedBy')";

if ($conn->query( $sql )) {
	echo json_encode(array("statusCode"=>200));
} 
else {
	echo json_encode(array("statusCode"=>201));
}

?>