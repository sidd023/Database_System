package Task3;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;

public class dbload {

	/*
	 * This function is called to implement writing data in bytes into a Heapfile
	 * 
	 */
	public static void main(String args[]) throws IOException {

		/*
		 * The file input and page size are taken as input
		 */
		String filename = "";
		int page_size = 0;
		if (args.length == 2) {
			page_size = Integer.parseInt(args[0]);
			filename = args[1];

		} else {
			// If no file is given, the program breaks
			System.out.println("NO FILE");
			return;
		}

		String filepath = filename;

		// to keep track of bytes used for a single page
		int bytes_used = 0;

		// to keep track of size of each record
		int record_size = 0;

		// page number starts with 0
		int page_no = 0;
		int offset = 0;

		// file name is initiated
		String heapfile = String.format("heapfile.%s", page_size);

		// this array keeps all the record read is used to keep records in code until
		// the page is filled
		byte[] ped_records = new byte[page_size];

		boolean first_line = true;

		// variable to keep track of objects used of a class
		int record_read = 0;

		// variables to keep track of time
		long total_records = 0;
		long endTime = 0;
		long startTime = 0;
		/*
		 * a class object is initiated with size of maximum page size as no records
		 * exceeding page size can be written into a page
		 */
		Record_Details[] record = new Record_Details[page_size];

		/*
		 * vufferReader to open the filepath and read each row
		 */
		BufferedReader csvReader = new BufferedReader(new FileReader(filepath));
		String row;
		String[] data = null;

		// time is noted before start of execution of heap
		startTime = System.currentTimeMillis();

		// iterates until all records are read
		while ((row = csvReader.readLine()) != null) {

			// first line (header) is omitted while reading the file
			if (first_line == false) {

				data = row.split(",");

				/*
				 * since the field hourly_Counts has integer values with ',' separated, the
				 * field is broken into two fields as we use ',' as delimiter
				 * 
				 * Hence, this code checks if Hourly_counts is been split into two and joins the
				 * combined field into one single variable
				 */
				if (data.length == 11) {
					String temp = data[9];

					temp = temp + data[10];
					temp = temp.replace("\"", "");
					data[9] = temp;
				}

				/*
				 * each input data are initialized accordingly using a class (more explanation
				 * in Record_Details.java)
				 */
				record[record_read] = new Record_Details();
				record[record_read].setID(data[0]);
				record[record_read].setDate_Time(data[1]);
				record[record_read].setYear(data[2]);
				record[record_read].setMonth(data[3]);
				record[record_read].setMdate(data[4]);
				record[record_read].setDay(data[5]);
				record[record_read].setTime(data[6]);
				record[record_read].setSensor_ID(data[7]);
				record[record_read].setSensor_Name(data[8]);
				record[record_read].setHourly_Counts(data[9]);
				record[record_read].setSTD_NAME(data[7], data[1]);

				// total size of this record is calculated
				record_size = record[record_read].get_total_bytes();

				/*
				 * combining the record size calculated above and the total bytes already read
				 * exceeds the page size, the array is written into a file before adding the
				 * newly read record into the array
				 */
				if (bytes_used + record_size > page_size) {

					/*
					 * page offsets are calculated accordingly in-order to write into a disk
					 */
					page_no += 1;
					offset = page_no * page_size;
					RandomAccessFile raf = null;
					try {
						raf = new RandomAccessFile(heapfile, "rw");
						raf.seek((long) offset);
						raf.write(ped_records, 0, ped_records.length - 1);

					} catch (FileNotFoundException e) {
						e.printStackTrace();
					} finally {
						raf.close();
					}
					/*
					 * The number of total records read is added each time an array is written into
					 * a file
					 * 
					 * Here, total bytes used and total record read are initialized to 0 as it make
					 * it easier to read in the next set of records until it reaches maximum page
					 * size
					 * 
					 * since all the records stored in the array ped_records are written into disk,
					 * it is initialized to a new set of records to continue reading
					 */
					total_records += record_read;
					bytes_used = 0;
					record_read = 0;
					ped_records = new byte[page_size];
				}
				/*
				 * If there is more space left in the page, i.e., as mentioned above if there is
				 * space to accommodate more records in the page, the records are added into
				 * ped_records that is stored locally in the code, which is written out once the
				 * page is full
				 */
				bytes_used += record_size;

//				String[] both = Stream.concat(Arrays.stream(a), Arrays.stream(b))
//	                      .toArray(String[]::new);

				/*
				 * The above read record is combined with the array lost containing all the
				 * records
				 */
				byte[] this_rec = new Record_Details().get_byte_data(record[record_read], page_size);
				byte[] one = this_rec;
				byte[] two = ped_records;
				byte[] combined = new byte[one.length + two.length];
				System.arraycopy(one, 0, combined, 0, one.length);
				System.arraycopy(two, 0, combined, one.length, two.length);
				ped_records = combined;

				record_read += 1;
			}
			// used to skip the first line of CSV
			first_line = false;
		}
		// time taken for all this to execute in noted
		endTime = System.currentTimeMillis();
		csvReader.close();

		// desired output are printed onto console
		System.out.println("Total Records loaded " + total_records);
		System.out.println("Total Pages used " + page_no);
		System.out.println("That took " + (endTime - startTime) + " milliseconds to ceate heapfile");

	}
}
