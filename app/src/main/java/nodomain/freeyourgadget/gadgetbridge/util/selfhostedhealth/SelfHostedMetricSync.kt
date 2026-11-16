/*  Copyright (C) 2026 Gadgetbridge contributors

    This file is part of Gadgetbridge.

    Gadgetbridge is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as published
    by the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    Gadgetbridge is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>. */
package nodomain.freeyourgadget.gadgetbridge.util.selfhostedhealth

import nodomain.freeyourgadget.gadgetbridge.GBApplication
import nodomain.freeyourgadget.gadgetbridge.devices.TimeSampleProvider
import nodomain.freeyourgadget.gadgetbridge.impl.GBDevice
import nodomain.freeyourgadget.gadgetbridge.model.TimeSample
import org.json.JSONArray
import org.json.JSONObject
import java.time.Instant
import java.time.ZoneId

internal object SelfHostedMetricSync {
    // The receiver accepts up to 2,000 extended samples per request.
    private const val MAX_METRICS_PER_REQUEST = 2_000

    private data class Sample(val metric: String, val timestampMillis: Long, val value: Int)

    fun attach(
        payload: SelfHostedHealthPayloadSet,
        device: GBDevice,
        fromSeconds: Long,
        toSeconds: Long,
        zone: ZoneId
    ): SelfHostedHealthPayloadSet {
        val fromMillis = fromSeconds * 1_000
        val toMillis = toSeconds * 1_000 + 999
        val samples = GBApplication.acquireDbReadOnly().use { db ->
            val coordinator = device.deviceCoordinator
            val session = db.daoSession
            read(coordinator.getSpo2SampleProvider(device, session), fromMillis, toMillis, "spo2", 1..100) { it.spo2 } +
                read(coordinator.getStressSampleProvider(device, session), fromMillis, toMillis, "stress", 0..100) { it.stress } +
                read(coordinator.getHrvValueSampleProvider(device, session), fromMillis, toMillis, "hrv", 0..1_000) { it.value }
        }
        if (samples.isEmpty()) return payload

        val metricsByDay = samples
            .distinctBy { it.metric to it.timestampMillis }
            .sortedBy { it.timestampMillis }
            .groupBy { Instant.ofEpochMilli(it.timestampMillis).atZone(zone).toLocalDate().toString() }
        val existingDays = payload.days.associateBy { it.date }
        val days = (existingDays.keys + metricsByDay.keys).sorted().flatMap { date ->
            val existing = existingDays[date]
            val metrics = metricsByDay[date] ?: return@flatMap listOfNotNull(existing)
            metrics.chunked(MAX_METRICS_PER_REQUEST).mapIndexed { index, batch ->
                val body = if (index == 0 && existing != null) {
                    JSONObject(existing.body.toString())
                } else {
                    JSONObject().put("date", date)
                }
                val entries = JSONArray()
                for (sample in batch) {
                    entries.put(
                        JSONObject().put("metric", sample.metric)
                            .put("timestamp", Instant.ofEpochMilli(sample.timestampMillis).toString())
                            .put("value", sample.value)
                    )
                }
                SelfHostedHealthDay(date, body.put("metrics", entries))
            }
        }
        return SelfHostedHealthPayloadSet(days, payload.sleepUploadedThrough)
    }

    private fun <T : TimeSample> read(
        provider: TimeSampleProvider<T>?,
        fromMillis: Long,
        toMillis: Long,
        metric: String,
        allowed: IntRange,
        valueOf: (T) -> Int
    ): List<Sample> = provider?.getAllSamples(fromMillis, toMillis).orEmpty().mapNotNull {
        val value = valueOf(it)
        if (it.timestamp !in fromMillis..toMillis || value !in allowed) {
            null
        } else {
            Sample(metric, it.timestamp, value)
        }
    }
}
