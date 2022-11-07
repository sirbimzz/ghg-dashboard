<?php 
include 'config/conn.php';

  $query = 'SELECT * FROM Non_Bonny_Offices';   
  $stmt = $conn->query( $query );
  $data = $stmt->fetchAll( PDO::FETCH_ASSOC );

  echo json_encode($data);

?>