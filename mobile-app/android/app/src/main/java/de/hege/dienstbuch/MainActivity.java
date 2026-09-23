package de.hege.dienstbuch;

import android.Manifest;
import android.content.Intent;
import android.content.pm.PackageManager;
import android.os.Build;
import android.os.Bundle;
import android.widget.Button;

import androidx.core.app.ActivityCompat;
import androidx.core.content.ContextCompat;

import com.getcapacitor.BridgeActivity;

public class MainActivity extends BridgeActivity {

    @Override
    public void onCreate(Bundle savedInstanceState) {
        registerPlugin(FcmTokenPlugin.class);
        super.onCreate(savedInstanceState);

        Button switchBtn = findViewById(R.id.switchServerBtn);
        if (switchBtn != null) {
            switchBtn.setOnClickListener(v -> {
                String localOrigin = this.getBridge().getLocalUrl();
                this.getBridge().getWebView().loadUrl(localOrigin + "/index.html?switch=1");
            });
        }

        requestNotificationPermissionIfNeeded();
        handleOpenUrlExtra(getIntent());
    }

    @Override
    protected void onNewIntent(Intent intent) {
        super.onNewIntent(intent);
        setIntent(intent);
        handleOpenUrlExtra(intent);
    }

    private void handleOpenUrlExtra(Intent intent) {
        if (intent == null) return;
        String openUrl = intent.getStringExtra("open_url");
        if (openUrl == null || openUrl.isEmpty()) return;
        intent.removeExtra("open_url");
        String escaped = openUrl.replace("\\", "\\\\").replace("'", "\\'");
        String js = "window.location.href = new URL('" + escaped + "', window.location.origin).href;";
        this.getBridge().getWebView().post(() -> this.getBridge().getWebView().evaluateJavascript(js, null));
    }

    private void requestNotificationPermissionIfNeeded() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
            if (ContextCompat.checkSelfPermission(this, Manifest.permission.POST_NOTIFICATIONS) != PackageManager.PERMISSION_GRANTED) {
                ActivityCompat.requestPermissions(this, new String[]{Manifest.permission.POST_NOTIFICATIONS}, 4321);
            }
        }
    }
}
