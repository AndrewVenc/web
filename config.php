<?php
declare(strict_types=1);

$dbhost = "127.0.0.1";
$dbuser = "vencon23";
$dbpass = "b5aQTDJg";
$dbname = "vencon23";

const PERMISSIVE_MODE = true;

function dir_mode(): int  { return PERMISSIVE_MODE ? 0777 : 0775; }
function file_mode(): int { return PERMISSIVE_MODE ? 0777 : 0664; }

const ENABLE_SETGID_ON_DIRS = true;


function ensure_dir(string $path): void {
    if (!is_dir($path)) {
        @mkdir($path, dir_mode(), true);
    }
    if (ENABLE_SETGID_ON_DIRS) {
        $mode = dir_mode();
        if (($mode & 02000) === 0) { $mode |= 02000; }
        @chmod($path, $mode);
    } else {
        @chmod($path, dir_mode());
    }
}

function write_file(string $path, string $data): void {
    $dir = dirname($path);
    ensure_dir($dir);

    $tmp = $path . '.tmp-' . bin2hex(random_bytes(6));
    if (@file_put_contents($tmp, $data) === false) {
        throw new RuntimeException("Nelze zapsat dočasný soubor: $tmp");
    }
    @chmod($tmp, file_mode());
    if (!@rename($tmp, $path)) {
        @unlink($tmp);
        throw new RuntimeException("Rename selhal: $path");
    }
    @chmod($path, file_mode());
}


const STORAGE_DIR   = __DIR__ . '/storage';
const LOG_FILE      = STORAGE_DIR . '/log.log';
const DATA_URL     = 'https://data.lacity.org/api/views/2nrs-mtv8/rows.csv?accessType=DOWNLOAD';
const DATA_CSV     = STORAGE_DIR . '/Crime_Data_from_2020_to_Present.csv';
const DATA_HASH    = STORAGE_DIR . '/last_hash.txt';
const DATA_TABLE   = 'open_data';
const IMPORT_MAX_ROWS = null;

ensure_dir(STORAGE_DIR);


function db_conn(): mysqli {
    static $conn = null;
    if ($conn instanceof mysqli) return $conn;

    global $dbhost, $dbuser, $dbpass, $dbname;
    mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT);

    $conn = mysqli_init();
    @mysqli_options($conn, MYSQLI_OPT_LOCAL_INFILE, 1);
    if (!@mysqli_real_connect($conn, $dbhost, $dbuser, $dbpass, $dbname)) {
        die("DB nepřipojena.");
    }
    mysqli_set_charset($conn, 'utf8mb4');
    return $conn;
}


function log_event(string $level, string $message, array $context = []): void {
    ensure_dir(STORAGE_DIR);

    $ts   = (new DateTimeImmutable('now', new DateTimeZone('Europe/Prague')))->format('Y-m-d H:i:s');
    $line = sprintf('[%s] [%s] %s', $ts, strtoupper($level), $message);
    if ($context) $line .= ' ' . json_encode($context, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
    $line .= PHP_EOL;

    $firstCreate = !file_exists(LOG_FILE);
    $fp = @fopen(LOG_FILE, 'ab');
    if ($fp === false) {
        @file_put_contents(LOG_FILE, $line, FILE_APPEND);
        if ($firstCreate) @chmod(LOG_FILE, file_mode());
        return;
    }
    flock($fp, LOCK_EX);
    fwrite($fp, $line);
    fflush($fp);
    flock($fp, LOCK_UN);
    fclose($fp);

    if ($firstCreate) @chmod(LOG_FILE, file_mode());
}
