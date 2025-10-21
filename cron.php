<?php
declare(strict_types=1);

// Controller pro celý běh přes web:
//  - step=fetch  (implicitně) stáhne CSV
//  - step=import spustí dávkový import s auto-refresh

require_once __DIR__ . '/config.php';

$step = $_GET['step'] ?? 'fetch';

if ($step === 'fetch') {
    // 1) Stáhni CSV (nebude se spouštět při dalších auto-refresh krocích)
    require_once __DIR__ . '/fetch_data.php';

    // 2) Přesměruj na import (auto)
    $next = htmlspecialchars($_SERVER['PHP_SELF'] . '?step=import', ENT_QUOTES, 'UTF-8');
    header('Content-Type: text/html; charset=utf-8');
    echo "<!doctype html><meta charset='utf-8'>
<meta http-equiv='refresh' content='1;url={$next}'>
<h1>Staženo. Připravuji import…</h1>
<p>Za okamžik pokračuji na import.</p>";
    exit;
}

if ($step === 'import') {
    // 3) Import – řekneme import skriptu, aby se po dávce vracel ZPĚT sem (ne na sebe)
    define('REFRESH_URL', $_SERVER['PHP_SELF'] . '?step=import');
    require_once __DIR__ . '/import_data.php';
    // import_data.php vypíše průběh/Hotovo a případně meta refresh na REFRESH_URL
    exit;
}

// fallback
http_response_code(400);
header('Content-Type: text/plain; charset=utf-8');
echo "Unknown step.";
