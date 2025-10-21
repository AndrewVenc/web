<?php


$dbhost = "127.0.0.1";
$dbuser = "vencon23";
$dbpass = "b5aQTDJg";
$dbname = "vencon23";

$conn = mysqli_connect($dbhost, $dbuser, $dbpass, $dbname);
if (!$conn) {
    die("Something went wrong. Database is not connected.");
}
?>