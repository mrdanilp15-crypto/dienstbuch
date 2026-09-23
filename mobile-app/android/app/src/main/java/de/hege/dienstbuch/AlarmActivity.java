package de.hege.dienstbuch;

import android.content.Context;
import android.content.Intent;
import android.media.AudioAttributes;
import android.media.AudioManager;
import android.media.MediaPlayer;
import android.media.RingtoneManager;
import android.os.Build;
import android.os.Bundle;
import android.os.VibrationEffect;
import android.os.Vibrator;
import android.view.WindowManager;
import android.widget.Button;
import android.widget.TextView;

import androidx.appcompat.app.AppCompatActivity;

/**
 * Vollbild-Alarmscreen, der über die Notification aus DienstbuchMessagingService gestartet
 * wird - auch bei gesperrtem Display. Läuft bewusst nativ statt als Web-Seite: das
 * Sperrbildschirm-Verhalten (setShowWhenLocked/setTurnScreenOn) und ein dauerhafter,
 * unterbrechbarer Alarmton lassen sich in einer WebView nicht zuverlässig umsetzen.
 */
public class AlarmActivity extends AppCompatActivity {

    private MediaPlayer mediaPlayer;
    private Vibrator vibrator;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);

        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O_MR1) {
            setShowWhenLocked(true);
            setTurnScreenOn(true);
        } else {
            getWindow().addFlags(
                WindowManager.LayoutParams.FLAG_SHOW_WHEN_LOCKED |
                WindowManager.LayoutParams.FLAG_TURN_SCREEN_ON |
                WindowManager.LayoutParams.FLAG_KEEP_SCREEN_ON |
                WindowManager.LayoutParams.FLAG_DISMISS_KEYGUARD
            );
        }
        getWindow().addFlags(WindowManager.LayoutParams.FLAG_KEEP_SCREEN_ON);

        setContentView(R.layout.activity_alarm);

        String title = getIntent().getStringExtra("title");
        String body = getIntent().getStringExtra("body");
        final String url = getIntent().getStringExtra("url");

        TextView titleView = findViewById(R.id.alarmTitle);
        TextView bodyView = findViewById(R.id.alarmBody);
        titleView.setText(title != null ? title : "Einsatzalarm");
        bodyView.setText(body != null ? body : "");

        Button viewBtn = findViewById(R.id.alarmViewBtn);
        Button dismissBtn = findViewById(R.id.alarmDismissBtn);

        viewBtn.setOnClickListener(v -> {
            stopAlarmSound();
            Intent i = new Intent(this, MainActivity.class);
            i.addFlags(Intent.FLAG_ACTIVITY_NEW_TASK | Intent.FLAG_ACTIVITY_CLEAR_TOP);
            if (url != null) i.putExtra("open_url", url);
            startActivity(i);
            finish();
        });

        dismissBtn.setOnClickListener(v -> {
            stopAlarmSound();
            finish();
        });

        startAlarmSound();
    }

    private void startAlarmSound() {
        try {
            mediaPlayer = new MediaPlayer();
            mediaPlayer.setAudioAttributes(new AudioAttributes.Builder()
                    .setUsage(AudioAttributes.USAGE_ALARM)
                    .setContentType(AudioAttributes.CONTENT_TYPE_SONIFICATION)
                    .build());
            android.net.Uri sound = RingtoneManager.getActualDefaultRingtoneUri(this, RingtoneManager.TYPE_ALARM);
            if (sound == null) sound = RingtoneManager.getDefaultUri(RingtoneManager.TYPE_NOTIFICATION);
            mediaPlayer.setDataSource(this, sound);
            mediaPlayer.setLooping(true);
            mediaPlayer.prepare();
            mediaPlayer.start();
        } catch (Exception ignored) {}

        vibrator = (Vibrator) getSystemService(Context.VIBRATOR_SERVICE);
        if (vibrator != null && vibrator.hasVibrator()) {
            long[] pattern = {0, 800, 500};
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                vibrator.vibrate(VibrationEffect.createWaveform(pattern, 0));
            } else {
                vibrator.vibrate(pattern, 0);
            }
        }
    }

    private void stopAlarmSound() {
        if (mediaPlayer != null) {
            try { mediaPlayer.stop(); mediaPlayer.release(); } catch (Exception ignored) {}
            mediaPlayer = null;
        }
        if (vibrator != null) {
            vibrator.cancel();
        }
    }

    @Override
    protected void onDestroy() {
        stopAlarmSound();
        super.onDestroy();
    }
}
