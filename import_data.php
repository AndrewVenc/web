<?php
declare(strict_types=1);

require_once __DIR__ . '/config.php';

if (!defined('REFRESH_URL')) {
    define('REFRESH_URL', $_SERVER['PHP_SELF']);
}



const CHECKPOINT_FILE   = STORAGE_DIR . '/import_checkpoint.json';
const BATCH_ROWS        = 50000;
const REFRESH_SECONDS   = 2;


function load_checkpoint(): array {
    if (!is_file(CHECKPOINT_FILE)) return ['pos'=>null,'rows'=>0,'started'=>false,'finished'=>false];
    $j = json_decode((string)@file_get_contents(CHECKPOINT_FILE), true);
    if (!is_array($j)) return ['pos'=>null,'rows'=>0,'started'=>false,'finished'=>false];
    return array_merge(['pos'=>null,'rows'=>0,'started'=>false,'finished'=>false], $j);
}
function save_checkpoint(?int $pos, int $rows, bool $started, bool $finished): void {
    write_file(CHECKPOINT_FILE, json_encode(['pos'=>$pos,'rows'=>$rows,'started'=>$started,'finished'=>$finished]));
}
function clear_checkpoint(): void { @unlink(CHECKPOINT_FILE); }

function toDate(?string $s): ?string {
    if ($s === null || $s === '') return null;
    $t = strtotime($s);
    return $t ? date('Y-m-d', $t) : null;
}
function toIntOrNull(?string $s): ?int {
    if ($s === null) return null;
    $s = trim($s);
    if ($s === '' || !is_numeric($s)) return null;
    return (int)$s;
}
function toFloatOrNull(?string $s): ?float {
    if ($s === null) return null;
    $s = trim($s);
    if ($s === '' || !is_numeric($s)) return null;
    return (float)$s;
}

try {
    ensure_dir(STORAGE_DIR);

    log_event('info', '--- IMPORT DATA START ---');

    if (!is_file(DATA_CSV)) {
        throw new RuntimeException('CSV nenalezeno – spusť nejdřív fetch_data.php.');
    }


    $fhHead = fopen(DATA_CSV, 'rb');
    if (!$fhHead) throw new RuntimeException('CSV nelze otevřít (head).');
    $headers = fgetcsv($fhHead, 0, ',', '"', '\\');
    fclose($fhHead);
    if (!$headers) throw new RuntimeException('Hlavička CSV je prázdná.');


    $map = [
        'DR_NO'         => 'dr_no',
        'Date Rptd'     => 'date_rptd',
        'DATE OCC'      => 'date_occ',
        'TIME OCC'      => 'time_occ',
        'AREA'          => 'area',
        'AREA NAME'     => 'area_name',
        'Rpt Dist No'   => 'rpt_dist_no',
        'Part 1-2'      => 'part_1_2',
        'Crm Cd'        => 'crm_cd',
        'Crm Cd Desc'   => 'crm_cd_desc',
        'Mocodes'       => 'mocodes',
        'Vict Age'      => 'vict_age',
        'Vict Sex'      => 'vict_sex',
        'Vict Descent'  => 'vict_descent',
        'Premis Cd'     => 'premis_cd',
        'Premis Desc'   => 'premis_desc',
        'Weapon Used Cd'=> 'weapon_used_cd',
        'Weapon Desc'   => 'weapon_desc',
        'Status'        => 'status',
        'Status Desc'   => 'status_desc',
        'Crm Cd 1'      => 'crm_cd_1',
        'Crm Cd 2'      => 'crm_cd_2',
        'Crm Cd 3'      => 'crm_cd_3',
        'Crm Cd 4'      => 'crm_cd_4',
        'LOCATION'      => 'location_text',
        'Cross Street'  => 'cross_street',
        'LAT'           => 'lat',
        'LON'           => 'lon',
    ];


    $idx = [];
    foreach ($headers as $i => $h) {
        $h = trim($h);
        if (isset($map[$h])) $idx[$map[$h]] = $i;
    }


    $cols = array_values($map);
    $colsBackticked = array_map(fn($c) => '`'.$c.'`', $cols);
    $ph   = '('.implode(',', array_fill(0, count($cols), '?')).')';
    $sql  = 'REPLACE INTO `'.DATA_TABLE.'` ('.implode(',', $colsBackticked).') VALUES '.$ph;

    $conn = db_conn();
    $stmt = mysqli_prepare($conn, $sql);
    if (!$stmt) throw new RuntimeException('Příprava REPLACE selhala: '.mysqli_error($conn));

    mysqli_begin_transaction($conn);


    $fh = fopen(DATA_CSV, 'rb');
    if (!$fh) throw new RuntimeException('CSV nelze otevřít.');

    fgetcsv($fh, 0, ',', '"', '\\');

    $cp = load_checkpoint();
    $totalRows = (int)$cp['rows'];

    if ($cp['pos'] !== null) {
        fseek($fh, $cp['pos']);
    }

    $types = str_repeat('s', count($cols));
    $rowsThisBatch = 0;
    $rowsThisTxn   = 0;

    while (($row = fgetcsv($fh, 0, ',', '"', '\\')) !== false) {
        $get = fn(string $dbCol) => array_key_exists($dbCol, $idx) ? ($row[$idx[$dbCol]] ?? null) : null;

        $v['dr_no']         = (string)$get('dr_no');
        $v['date_rptd']     = toDate($get('date_rptd'));
        $v['date_occ']      = toDate($get('date_occ'));
        $v['time_occ']      = ($tmp = toIntOrNull($get('time_occ'))) === null ? null : (string)$tmp;
        $v['area']          = ($tmp = toIntOrNull($get('area'))) === null ? null : (string)$tmp;
        $v['area_name']     = $get('area_name');
        $v['rpt_dist_no']   = ($tmp = toIntOrNull($get('rpt_dist_no'))) === null ? null : (string)$tmp;
        $v['part_1_2']      = ($tmp = toIntOrNull($get('part_1_2'))) === null ? null : (string)$tmp;
        $v['crm_cd']        = ($tmp = toIntOrNull($get('crm_cd'))) === null ? null : (string)$tmp;
        $v['crm_cd_desc']   = $get('crm_cd_desc');
        $v['mocodes']       = $get('mocodes');
        $v['vict_age']      = ($tmp = toIntOrNull($get('vict_age'))) === null ? null : (string)$tmp;
        $v['vict_sex']      = $get('vict_sex');
        $v['vict_descent']  = $get('vict_descent');
        $v['premis_cd']     = ($tmp = toIntOrNull($get('premis_cd'))) === null ? null : (string)$tmp;
        $v['premis_desc']   = $get('premis_desc');
        $v['weapon_used_cd']= ($tmp = toIntOrNull($get('weapon_used_cd'))) === null ? null : (string)$tmp;
        $v['weapon_desc']   = $get('weapon_desc');
        $v['status']        = $get('status');
        $v['status_desc']   = $get('status_desc');
        $v['crm_cd_1']      = ($tmp = toIntOrNull($get('crm_cd_1'))) === null ? null : (string)$tmp;
        $v['crm_cd_2']      = ($tmp = toIntOrNull($get('crm_cd_2'))) === null ? null : (string)$tmp;
        $v['crm_cd_3']      = ($tmp = toIntOrNull($get('crm_cd_3'))) === null ? null : (string)$tmp;
        $v['crm_cd_4']      = ($tmp = toIntOrNull($get('crm_cd_4'))) === null ? null : (string)$tmp;
        $v['location_text'] = $get('location_text');
        $v['cross_street']  = $get('cross_street');
        $v['lat']           = ($tmp = toFloatOrNull($get('lat'))) === null ? null : (string)$tmp;
        $v['lon']           = ($tmp = toFloatOrNull($get('lon'))) === null ? null : (string)$tmp;


        $vals = array_values($v);
        $bind = [$types];
        foreach ($vals as $k => $val) { $bind[] = &$vals[$k]; }
        call_user_func_array('mysqli_stmt_bind_param', array_merge([$stmt], $bind));

        if (!mysqli_stmt_execute($stmt)) {
            throw new RuntimeException('Chyba REPLACE: '.mysqli_stmt_error($stmt));
        }

        $rowsThisBatch++;
        $rowsThisTxn++;
        $totalRows++;

        if ($rowsThisTxn >= 5000) {
            mysqli_commit($conn);
            mysqli_begin_transaction($conn);
            $rowsThisTxn = 0;
            log_event('info', 'Průběžný commit', ['rows' => $totalRows]);
        }

        if ($rowsThisBatch >= BATCH_ROWS) {
            $pos = ftell($fh);
            mysqli_commit($conn);
            save_checkpoint($pos, $totalRows, true, false);
            fclose($fh);
            mysqli_stmt_close($stmt);
            log_event('info', 'BATCH DONE', ['total_rows'=>$totalRows,'pos'=>$pos]);


            header('Content-Type: text/html; charset=utf-8');
            echo "<!doctype html><meta charset='utf-8'><meta http-equiv='refresh' content='".REFRESH_SECONDS.";url=".htmlspecialchars(REFRESH_URL, ENT_QUOTES, 'UTF-8')."'>
            <h1>Probíhá import…</h1>
            <p>Načteno celkem: <strong>".number_format($totalRows, 0, ',', ' ')."</strong> řádků.</p>
            <p>Za pár vteřin to automaticky pokračuje… (nech tuhle stránku otevřenou)</p>";
            return;
        }
    }

    fclose($fh);
    mysqli_commit($conn);
    mysqli_stmt_close($stmt);
    clear_checkpoint();
    log_event('info', 'IMPORT DONE', ['rows' => $totalRows]);

    header('Content-Type: text/html; charset=utf-8');
    echo "<!doctype html><meta charset='utf-8'>
    <h1>Hotovo ✅</h1>
    <p>Načteno celkem: <strong>".number_format($totalRows, 0, ',', ' ')."</strong> řádků.</p>
    <p>Můžeš zavřít okno.</p>";

} catch (Throwable $e) {
    log_event('error', 'Import error', ['exception' => $e->getMessage()]);
    http_response_code(500);
    header('Content-Type: text/plain; charset=utf-8');
    echo "ERROR: ".$e->getMessage().PHP_EOL;
    exit(1);
}
