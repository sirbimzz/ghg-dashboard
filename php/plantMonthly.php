<?php 
include 'config/conn.php';

  $query = 'SELECT * FROM GHG_Plant_Monthly';   
  $stmt = $conn->query( $query );
  $data = $stmt->fetchAll( PDO::FETCH_ASSOC );

  echo json_encode($data);

?>