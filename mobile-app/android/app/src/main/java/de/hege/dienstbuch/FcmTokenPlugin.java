package de.hege.dienstbuch;

import android.app.Activity;
import android.content.Context;
import android.content.Intent;
import android.net.Uri;
import android.os.Build;
import android.os.PowerManager;
import android.provider.Settings;

import com.getcapacitor.JSObject;
import com.getcapacitor.Plugin;
import com.getcapacitor.PluginCall;
import com.getcapacitor.PluginMethod;
import com.getcapacitor.annotation.CapacitorPlugin;
import com.google.firebase.messaging.FirebaseMessaging;

/**
 * Brücke zwischen der (je nach Feuerwehr wechselnden) Web-Oberfläche und den nativen
 * FCM-/Akku-APIs. Bewusst ein eigenes schlankes Plugin statt @capacitor/push-notifications:
 * der Empfang läuft komplett über die eigene DienstbuchMessagingService (Vollbild-Alarm),
 * dieses Plugin liefert der Web-Seite nur den aktuellen Token, damit sie ihn beim jeweils
 * verbundenen Server registrieren kann (siehe POST /api/push/register-fcm-token).
 */
@CapacitorPlugin(name = "FcmToken")
public class FcmTokenPlugin extends Plugin {

    @PluginMethod
    public void getToken(PluginCall call) {
        FirebaseMessaging.getInstance().getToken().addOnCompleteListener(task -> {
            if (!task.isSuccessful()) {
                call.reject("FCM-Token konnte nicht abgerufen werden", task.getException());
                return;
            }
            JSObject ret = new JSObject();
            ret.put("token", task.getResult());
            call.resolve(ret);
        });
    }

    @PluginMethod
    public void isIgnoringBatteryOptimizations(PluginCall call) {
        JSObject ret = new JSObject();
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
            PowerManager pm = (PowerManager) getContext().getSystemService(Context.POWER_SERVICE);
            ret.put("ignoring", pm != null && pm.isIgnoringBatteryOptimizations(getContext().getPackageName()));
        } else {
            ret.put("ignoring", true);
        }
        call.resolve(ret);
    }

    @PluginMethod
    public void requestBatteryOptimizationExemption(PluginCall call) {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
            Activity activity = getActivity();
            Intent intent = new Intent(Settings.ACTION_REQUEST_IGNORE_BATTERY_OPTIMIZATIONS);
            intent.setData(Uri.parse("package:" + getContext().getPackageName()));
            activity.startActivity(intent);
        }
        call.resolve();
    }
}
