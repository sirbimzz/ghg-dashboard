<?php 

include 'config/conn.php';

$postElement=$_POST['postElement'];
$sql = "$postElement";

if ($conn->query( $sql )) {
	echo json_encode(array("statusCode"=>200));
} 
else {
	echo json_encode(array("statusCode"=>201));
}

?>