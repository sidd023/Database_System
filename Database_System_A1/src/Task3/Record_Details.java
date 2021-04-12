package Task3;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class Record_Details {

	/*
	 * the data are stored in byte[] format for each field
	 */
	private byte[] ID = new byte[4];

	private byte[] Date_Time;

	private byte[] Year = new byte[4];

	private byte[] Month;

	private byte[] Day;

	private byte[] Sensor_Name;

	private byte[] Mdate = new byte[4];

	private byte[] Time = new byte[4];

	private byte[] Sensor_ID = new byte[4];

	private byte[] Hourly_Counts = new byte[4];

	private byte[] STD_NAME;

	private byte delimiter = ';';

	private byte end_of_record = '~';

	// constructor
	public Record_Details() {
		super();

	}

	/*
	 * getters and setters for each fields are implemented
	 */
	public byte[] getSTD_NAME() {
		return STD_NAME;
	}

	public byte[] getID() {
		return ID;
	}

	public byte[] getDate_Time() {
		return Date_Time;
	}

	public byte[] getYear() {
		return Year;
	}

	public byte[] getMonth() {
		return Month;
	}

	public byte[] getDay() {
		return Day;
	}

	public byte[] getSensor_Name() {
		return Sensor_Name;
	}

	public byte[] getMdate() {
		return Mdate;
	}

	public byte[] getTime() {
		return Time;
	}

	public byte[] getSensor_ID() {
		return Sensor_ID;
	}

	public byte[] getHourly_Counts() {
		return Hourly_Counts;
	}

	public byte getDelimiter() {
		return delimiter;
	}

	public byte getEndofRecord() {
		return end_of_record;
	}

	/*
	 * Integer Fields The given data with int fields are stored in their ASCII value
	 * occupying 4 bytes of data
	 * 
	 * The passed parameter string is converted into string which is then converted
	 * and stored as bytes
	 */

	public void setYear(String year) {
		Year = int_to_byte_array(Integer.parseInt(year));
	}

	public void setMdate(String mdate) {
		Mdate = int_to_byte_array(Integer.parseInt(mdate));
	}

	public void setTime(String time) {
		Time = int_to_byte_array(Integer.parseInt(time));
	}

	public void setSensor_ID(String sensor_ID) {
		Sensor_ID = int_to_byte_array(Integer.parseInt(sensor_ID));
	}

	public void setHourly_Counts(String hourly_Counts) {
		Hourly_Counts = int_to_byte_array(Integer.parseInt(hourly_Counts.replace(",", "")));

	}

	/*
	 * String fields The passed string parameter is converted into binary format
	 * before converting them into byte[]
	 */

	public void setID(String iD) {
		ID = string_to_byte_array(strToBinary(iD));
	}

	public void setSTD_NAME(String sensor_ID, String DateTime) {
		STD_NAME = string_to_byte_array(strToBinary(sensor_ID + " " + DateTime));

	}

	public void setDate_Time(String date_Time) {
		Date_Time = string_to_byte_array(strToBinary(date_Time));

	}

	public void setMonth(String month) {
		Month = string_to_byte_array(strToBinary(month));

	}

	public void setDay(String day) {
		Day = string_to_byte_array(strToBinary(day));

	}

	public void setSensor_Name(String sensor_Name) {
		Sensor_Name = string_to_byte_array(strToBinary(sensor_Name));

	}

	/*
	 * This function converts the given integer into an array of bytes which is then
	 * returned
	 */
	public byte[] int_to_byte_array(int i) {
		ByteBuffer bb = ByteBuffer.allocate(4);
		bb.putInt(i);
		return bb.array();
	}

	/*
	 * this method converts the integer data field byts[] back into its original
	 * data, Integer
	 */
	public int byte_array_to_int(byte[] data) {
		return ByteBuffer.wrap(data).getInt();
	}

	/*
	 * This function is responsible to convert the passed string of binary data into
	 * bytes
	 */
	public byte[] string_to_byte_array(String string) {

		// the string is searched for whitespace and replaced
		string = string.replaceAll("\\s+", "");

		// the binary data is split into bits of 8 and converted
		List<Integer> list = new ArrayList<>();
		for (String str : string.split("(?<=\\G.{8})")) {
			list.add(Integer.parseInt(str, 2));
		}
		// data from the list is coppied into byte[] which
		// is then returned
		byte[] data = new byte[list.size()];
		for (int i = 0; i < list.size(); i++) {
			int bin = list.get(i);
			data[i] = (byte) bin;
		}
		return data;
	}

	/*
	 * This method converts the array byte of string fields back to its original
	 * data
	 */
	public String byte_array_to_string(byte[] data) {
		return new String(data);
	}

//	public String byte_to_string(byte data)
//	{
//		String s1 = String.format("%8s", Integer.toBinaryString(data & 0xFF)).replace(' ', '0');
//		return new String (string_to_byte_array(s1));
//	}

	/*
	 * The length of each record is computed by adding up the byte array size of
	 * each field
	 */
	public int get_total_bytes() {
		int tot_bytes = this.ID.length + this.Date_Time.length + this.Year.length + this.Month.length
				+ this.Mdate.length + this.Day.length + this.Time.length + this.Sensor_ID.length
				+ this.Hourly_Counts.length + this.Sensor_Name.length;
		return tot_bytes;
	}

	/*
	 * this function takes in the record and the page size which returns a record as
	 * a single byte array with delimiters and end of record character added
	 */
	public byte[] get_byte_data(Record_Details record, int record_size) {
		byte[] allByteArray = new byte[record_size];
		ByteBuffer buff = ByteBuffer.wrap(allByteArray);
		buff.put(record.getID());
		buff.put(delimiter);
		buff.put(record.getDate_Time());
		buff.put(delimiter);
		buff.put(record.getYear());
		buff.put(delimiter);
		buff.put(record.getMonth());
		buff.put(delimiter);
		buff.put(record.getMdate());
		buff.put(delimiter);
		buff.put(record.getDay());
		buff.put(delimiter);
		buff.put(record.getTime());
		buff.put(delimiter);
		buff.put(record.getSensor_ID());
		buff.put(delimiter);
		buff.put(record.getSensor_Name());
		buff.put(delimiter);
		buff.put(record.getHourly_Counts());
		buff.put(delimiter);
		buff.put(record.getSTD_NAME());
		buff.put(end_of_record);
		return buff.array();
	}

	/*
	 * This function converts the given string into string of binary data This
	 * string of binary data is returned
	 */
	public String strToBinary(String s) {
		int n = s.length();
		String bin_str = "";
		for (int i = 0; i < n; i++) {
			int val = Integer.valueOf(s.charAt(i));
			String bin = "";
			while (val > 0) {
				if (val % 2 == 1) {
					bin += '1';
				} else
					bin += '0';
				val /= 2;
			}
			bin = reverse(bin);
			while (bin.length() % 8 != 0) {
				bin = "0" + bin;
			}
			bin_str += bin + " ";
		}
		return bin_str;
	}

	/*
	 * this function is used within strToBinary function to convert the string into
	 * string of binary
	 */
	public static String reverse(String input) {
		char[] a = input.toCharArray();
		int l, r = 0;
		r = a.length - 1;

		for (l = 0; l < r; l++, r--) {

			char temp = a[l];
			a[l] = a[r];
			a[r] = temp;
		}
		return String.valueOf(a);
	}

}
