import com.epam.bigdata201.kstreams.model.HotelRecord;
import com.epam.bigdata201.kstreams.model.HotelRecordEnriched;
import com.epam.bigdata201.kstreams.model.WeatherRecord;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Test;


import static org.junit.Assert.*;

public class JsonTest {
    @Test
    public void testJsonDeserializationWithMapping() {
        String json = "{\"lng\":-90.9898,\"lat\":38.2149,\"avg_tmpr_f\":71.9,\"avg_tmpr_c\":22.2,\"wthr_date\":\"2017-08-28\"}";
        WeatherRecord targetObject = new Gson().fromJson(json, WeatherRecord.class);

        assertEquals(targetObject.getDate(), "2017-08-28");
    }

    @Test
    public void testNullWeather() {
        HotelRecord hotel = new HotelRecord("1", "Hilton", "USA", "New-York", "20.00", "30.00", "dfjd", "2020-09-09");
        HotelRecordEnriched hotelEnriched = hotel.addWeather(null);

        Gson gson = new GsonBuilder().serializeNulls().create();
        HotelRecordEnriched x = gson.fromJson("{" +
            "\"id\":\"515396075520\"," +
            "\"name\":\"Econo Lodge\",\"country\":\"US\"," +
            "\"city\":\"Bellingham\",\"latitude\":\"48.781123\",\"longitude\":\"-122.485944\",\"geohash\":\"c28v\",\"date\":\"2017-09-28\"}", HotelRecordEnriched.class);
        System.out.println(x);
        String hotelJson = gson.toJson(hotelEnriched, HotelRecordEnriched.class);
        System.out.println(hotelJson);
        assertNull(hotelEnriched.getAvgTemperatureC());
        assertNull(hotelEnriched.getAvgTemperatureF());
    }
}
