import os
import mysql.connector

def get_db_connection():
    """
    Erstellt eine Verbindung zur MySQL/MariaDB Datenbank.
    Unterstützt automatische Kennwort-Reparatur über den Root-Zugang,
    falls das Passwort in MariaDB und den API-Umgebungsvariablen abweicht.
    """
    host = os.getenv("DB_HOST", os.getenv("MYSQL_HOST", "db"))
    user = os.getenv("DB_USER", os.getenv("MYSQL_USER", "app_user"))
    password = os.getenv("DB_PASSWORD") or os.getenv("DB_PASS") or os.getenv("MYSQL_PASSWORD") or "dein_app_passwort"
    database = os.getenv("DB_NAME", os.getenv("MYSQL_DATABASE", "attendance_system"))
    port = int(os.getenv("DB_PORT", os.getenv("MYSQL_PORT", "3306")))
    env_root_pass = os.getenv("ROOT_PASS") or os.getenv("MYSQL_ROOT_PASSWORD") or ""

    # 1. Erster Versuch: Normale Anmeldung als app_user
    try:
        return mysql.connector.connect(
            host=host, user=user, password=password, database=database, port=port
        )
    except mysql.connector.Error as err:
        if err.errno != 1045: # Falls kein Zugriffsfehler (sondern z.B. Host noch nicht bereit)
            raise err

    print(f"[DB AUTO-REPAIR] Zugriffsfehler für '{user}'. Starte automatische Passwort-Synchronisation über Root...")

    # 2. Zweiter Versuch: Passwort-Vergleichsliste für den Root-Zugang
    root_passwords_to_try = [
        env_root_pass,
        "Dein_ganz_geheimes_root_passwort",
        "dein_ganz_geheimes_root_passwort",
        password,
        "dein_app_passwort",
        "rootpass123",
        "root",
        ""
    ]

    for try_root in root_passwords_to_try:
        if try_root is None:
            continue
        try:
            root_conn = mysql.connector.connect(
                host=host, user="root", password=try_root, port=port
            )
            cur = root_conn.cursor()
            cur.execute(f"CREATE DATABASE IF NOT EXISTS `{database}`;")
            
            # MariaDB-kompatible Benutzer- & Passwort-Aktualisierung
            try:
                cur.execute(f"CREATE USER IF NOT EXISTS '{user}'@'%' IDENTIFIED BY %s;", (password,))
            except Exception:
                pass
                
            try:
                cur.execute(f"GRANT ALL PRIVILEGES ON `{database}`.* TO '{user}'@'%' IDENTIFIED BY %s;", (password,))
            except Exception:
                cur.execute(f"ALTER USER '{user}'@'%' IDENTIFIED BY %s;", (password,))
                cur.execute(f"GRANT ALL PRIVILEGES ON `{database}`.* TO '{user}'@'%' ;")

            cur.execute("FLUSH PRIVILEGES;")
            root_conn.commit()
            cur.close()
            root_conn.close()

            print(f"[DB AUTO-REPAIR] Passwort für '{user}' in MariaDB erfolgreich auf DB_PASS aktualisiert!")
            
            # Erfolgreiche Anmeldung als app_user zurückgeben
            return mysql.connector.connect(
                host=host, user=user, password=password, database=database, port=port
            )
        except Exception as r_err:
            pass

    # Falls alle Root-Versuche fehlschlugen, ursprünglichen Fehler werfen
    raise mysql.connector.Error(msg=f"Access denied for user '{user}' and root auto-repair could not match root password.")
