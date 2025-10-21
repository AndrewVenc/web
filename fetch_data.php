<?php
declare(strict_types=1);

require_once __DIR__ . '/config.php';


try {
    log_event('info', '--- FETCH DATA START (stream) ---', ['url' => DATA_URL]);

    ensure_dir(STORAGE_DIR);

    $ctx = stream_context_create([
        'http' => [
            'timeout'       => 500,
            'ignore_errors' => true,
        ],
        'ssl'  => [
            'verify_peer'      => true,
            'verify_peer_name' => true,
        ],
    ]);


    $in = @fopen(DATA_URL, 'rb', false, $ctx);
    if ($in === false) {
        throw new RuntimeException('Nelze otevřít vzdálený stream (fopen).');
    }


    $tmpPath = DATA_CSV . '.part-' . bin2hex(random_bytes(6));
    $out = @fopen($tmpPath, 'wb');
    if ($out === false) {
        fclose($in);
        throw new RuntimeException('Nelze vytvořit dočasný soubor pro zápis.');
    }
    @chmod($tmpPath, file_mode());


    $hashCtx = hash_init('sha256');


    $bytes = 0;
    while (!feof($in)) {
        $buf = fread($in, 1024 * 1024);
        if ($buf === false) {
            fclose($in);
            fclose($out);
            @unlink($tmpPath);
            throw new RuntimeException('Chyba při čtení vzdáleného streamu.');
        }
        if ($buf === '') {
            continue;
        }

        $w = fwrite($out, $buf);
        if ($w === false) {
            fclose($in);
            fclose($out);
            @unlink($tmpPath);
            throw new RuntimeException('Chyba při zápisu do dočasného souboru.');
        }
        $bytes += $w;
        hash_update($hashCtx, $buf);
    }

    fclose($in);
    fflush($out);
    fclose($out);

    $newHash = hash_final($hashCtx);
    $oldHash = is_file(DATA_HASH) ? trim((string)@file_get_contents(DATA_HASH)) : '';


    if ($oldHash !== '' && hash_equals($oldHash, $newHash) && is_file(DATA_CSV)) {
        @unlink($tmpPath); // dočasák už nepotřebujeme
        log_event('info', 'Data unchanged – continue to import.', [
            'sha256' => $newHash,
            'bytes'  => $bytes
        ]);
        echo "Beze změny – pokračuju na import…\n";
        return; // <<< DŮLEŽITÉ: tím zastavíš fetch, aby dál nic nerenamoval
    }
    
    
    


    if (is_file(DATA_CSV)) {
        @chmod(DATA_CSV, file_mode());
        @unlink(DATA_CSV);
    }
    if (!@rename($tmpPath, DATA_CSV)) {
        @unlink($tmpPath);
        throw new RuntimeException('Nelze přejmenovat dočasný soubor na cílový.');
    }
    @chmod(DATA_CSV, file_mode());

    write_file(DATA_HASH, $newHash);

    log_event('info', 'CSV stored (stream)', ['bytes' => $bytes, 'sha256' => $newHash]);
    echo "Staženo a připraveno k importu.\n";

    log_event('info', '--- FETCH DATA END (stream) ---');
} catch (Throwable $e) {
    log_event('error', 'Fetch error (stream)', ['exception' => $e->getMessage()]);
    http_response_code(500);
    if (isset($tmpPath) && is_file($tmpPath)) {
        @unlink($tmpPath);
    }
    if (defined('STDERR')) {
        fwrite(STDERR, "ERROR: ".$e->getMessage().PHP_EOL);
    } else {
        echo "ERROR: ".$e->getMessage().PHP_EOL;
    }
    exit(1);
}
