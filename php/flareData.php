<?php 
include 'config/conn.php';

  $query = 'SELECT * FROM GHG_Flaring_Weekly';   
  $stmt = $conn->query( $query );
  $data = $stmt->fetchAll( PDO::FETCH_ASSOC );

  echo json_encode($data);

?>