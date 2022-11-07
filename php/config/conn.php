<?php 
$servername = "BNY-S-560";
$username = "Abimbola.Salami";
$password = "NLNG@3070";
$database = "dataEntryDB";
$port = "1433";
$conn="";
try {
    $conn = new PDO("sqlsrv:server=$servername;Database=$database;", $username, $password
    );
} catch (PDOException $e) {
    echo ("Error connecting to SQL Server: " . $e->getMessage());
}
//echo "Connected to SQL Server\n";  
?>