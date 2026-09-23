package de.hege.dienstbuch;

import android.app.Notification;
import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.content.Context;
import android.content.Intent;
import android.content.SharedPreferences;
import android.media.AudioAttributes;
import android.media.RingtoneManager;
import android.net.Uri;
import android.os.Build;

import androidx.core.app.NotificationCompat;
import androidx.core.app.NotificationManagerCompat;

import com.google.firebase.messaging.FirebaseMessagingService;
import com.google.firebase.messaging.RemoteMessage;

import java.util.Map;

/**
 * Empfängt Alarm-Push von Firebase Cloud Messaging. Bewusst eine eigene Service-Klasse statt
 * eines fertigen Plugins: nur so kann eine Vollbild-Benachrichtigung (wie ein eingehender
 * Anruf, weckt Display + spielt Ton auch bei gesperrtem Handy/App komplett beendet) gebaut
 * werden. Der Server schickt dafür bewusst eine reine Daten-Nachricht ohne "notification"-Feld
 * (siehe routers/push_api.py::send_fcm_to_all) - nur dann landet JEDE Nachricht hier in
 * onMessageReceived, unabhängig vom App-Zustand.
 */
public class DienstbuchMessagingService extends FirebaseMessagingService {

    public static final String CHANNEL_ID = "dienstbuch_alarm";
    private static final String PREFS_NAME = "dienstbuch_fcm";
    private static final String KEY_TOKEN = "fcm_token";

    @Override
    public void onNewToken(String token) {
        super.onNewToken(token);
        // Wird lokal zwischengespeichert; die eigentliche Übermittlung an den (je nach Wehr
        // unterschiedlichen) Server übernimmt die Web-Seite selbst über den FcmTokenPlugin,
        // sobald der Nutzer eingeloggt ist - hier ist nicht bekannt, mit welchem Server-Konto
        // dieses Gerät gerade verbunden ist.
        getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE).edit().putString(KEY_TOKEN, token).apply();
    }

    public static String getCachedToken(Context ctx) {
        return ctx.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE).getString(KEY_TOKEN, null);
    }

    @Override
    public void onMessageReceived(RemoteMessage message) {
        super.onMessageReceived(message);
        Map<String, String> data = message.getData();
        String title = data.getOrDefault("title", "Dienstbuch Alarm");
        String body = data.getOrDefault("body", "");
        String url = data.getOrDefault("url", "/dashboard");

        ensureChannel();

        Intent alarmIntent = new Intent(this, AlarmActivity.class);
        alarmIntent.addFlags(Intent.FLAG_ACTIVITY_NEW_TASK | Intent.FLAG_ACTIVITY_CLEAR_TOP | Intent.FLAG_ACTIVITY_SINGLE_TOP);
        alarmIntent.putExtra("title", title);
        alarmIntent.putExtra("body", body);
        alarmIntent.putExtra("url", url);

        int flags = PendingIntent.FLAG_UPDATE_CURRENT | (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M ? PendingIntent.FLAG_IMMUTABLE : 0);
        PendingIntent fullScreenPendingIntent = PendingIntent.getActivity(this, 0, alarmIntent, flags);

        // Nutzt den auf dem Gerät hinterlegten System-Alarmton statt einer eigenen Audiodatei -
        // der ist bewusst laut/durchdringend eingestellt (User kennt ihn vom Wecker) und
        // erspart das Mitliefern einer eigenen Sounddatei im APK.
        Uri alarmSound = RingtoneManager.getActualDefaultRingtoneUri(this, RingtoneManager.TYPE_ALARM);
        if (alarmSound == null) {
            alarmSound = RingtoneManager.getDefaultUri(RingtoneManager.TYPE_NOTIFICATION);
        }

        NotificationCompat.Builder builder = new NotificationCompat.Builder(this, CHANNEL_ID)
                .setSmallIcon(android.R.drawable.ic_dialog_alert)
                .setContentTitle(title)
                .setContentText(body)
                .setPriority(NotificationCompat.PRIORITY_MAX)
                .setCategory(NotificationCompat.CATEGORY_ALARM)
                .setAutoCancel(true)
                .setFullScreenIntent(fullScreenPendingIntent, true)
                .setContentIntent(fullScreenPendingIntent)
                .setVibrate(new long[]{0, 800, 500, 800, 500, 800})
                .setSound(alarmSound);

        NotificationManagerCompat.from(this).notify((int) System.currentTimeMillis(), builder.build());

        // Zusätzlich direkt starten: bei manchen Herstellern (Samsung u.a.) wird der
        // Vollbild-Intent aus der Notification heraus erst mit spürbarer Verzögerung
        // ausgelöst, oder unterdrückt, solange das Display bereits an ist. Ein direkter
        // Start (klappt zuverlässig, solange der Prozess läuft) verkürzt das im
        // Normalfall auf praktisch verzögerungsfrei.
        try {
            startActivity(alarmIntent);
        } catch (Exception ignored) {
            // Prozess evtl. vollständig beendet - dann übernimmt allein der FullScreenIntent der Notification.
        }
    }

    private void ensureChannel() {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.O) return;
        NotificationManager nm = getSystemService(NotificationManager.class);
        if (nm == null || nm.getNotificationChannel(CHANNEL_ID) != null) return;

        AudioAttributes attrs = new AudioAttributes.Builder()
                .setUsage(AudioAttributes.USAGE_ALARM)
                .setContentType(AudioAttributes.CONTENT_TYPE_SONIFICATION)
                .build();

        NotificationChannel channel = new NotificationChannel(CHANNEL_ID, "Einsatzalarm", NotificationManager.IMPORTANCE_HIGH);
        channel.setDescription("Alarmierung bei neuem Einsatz - weckt das Display und spielt Ton auch im Stromsparmodus.");
        channel.enableVibration(true);
        channel.setVibrationPattern(new long[]{0, 800, 500, 800, 500, 800});
        channel.setBypassDnd(true);
        channel.setLockscreenVisibility(Notification.VISIBILITY_PUBLIC);
        Uri alarmSound = RingtoneManager.getActualDefaultRingtoneUri(this, RingtoneManager.TYPE_ALARM);
        if (alarmSound == null) {
            alarmSound = RingtoneManager.getDefaultUri(RingtoneManager.TYPE_NOTIFICATION);
        }
        channel.setSound(alarmSound, attrs);
        nm.createNotificationChannel(channel);
    }
}
