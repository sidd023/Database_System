package Task3;

import java.io.FileNotFoundException; 
import java.io.IOException;
import java.io.RandomAccessFile;

public class dbquery {

	public static void main(String args[]) {

		/*
		 * The file input and page size are taken as input
		 */
		String input_search = "";
		int page_size = 0;
		if (args.length == 2) {
			input_search = args[0];
			page_size = Integer.parseInt(args[1]);
			
		} else {
			// If no file is given, the program breaks
			System.out.println("NO FILE\nPlease enter in format java dbquery text pagesize");
			return;
		}

		String heapfile = String.format("heapfile.%s", page_size);

		int pageNo = 0;
		boolean check = true;

		/*
		 * start time and end time are calculated
		 */
		long endTime = 0;
		long startTime = 0;
		startTime = System.currentTimeMillis();

		/*
		 * this loop breaks when an exception is thrown, when there are no more pages to
		 * read from the heap
		 */
		while (check == true) {
			// page number and offsets are calculated
			pageNo += 1;
			int offset = page_size * pageNo;
			byte[] data = new byte[page_size];

			RandomAccessFile raf = null;
			try {
				raf = new RandomAccessFile(heapfile, "r");
				raf.seek((long) offset);

				// a page is read into data byte[]
				raf.read(data, 0, page_size);
			} catch (FileNotFoundException e) {
				System.out.println(" File Not Found");
			} catch (IOException e) {
				endTime = System.currentTimeMillis();
				System.out.println("Time taken to search records " + (endTime - startTime) + " ms.");
				System.out.println("Pages Read " + pageNo);
				check = false;
				break;
			}

			finally {
				try {
					raf.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			/*
			 * the collection of records are converted into string format making it easier
			 * to read
			 */
			String whole_records = new String(data);

			/*
			 * each record from the pool of records are split into individual arrays using
			 * the delimiter
			 */
			String[] each_record = whole_records.split("~");

			/*
			 * since there are more then one record in a page, we iterate through each
			 * record array
			 */
			for (int i = 0; i < each_record.length; i++) {

				/*
				 * each field of data from a single record is split
				 */
				String[] rec = each_record[i].split(";");
				String id = rec[0];
				String search = rec[rec.length - 1];

				/*
				 * the search is performed over the data field to check if there exists any data
				 * with the given input
				 */
				if (search.contains(input_search)) {
					// data is displayed to the console
					System.out.println("Found " + input_search + " at record ID " + id + " STD_NAME " + search);
				}

			} // iterate through each record and fins for the match

		} // the loop breaks with the while loop when there are no more records left in
			// the heap
	}

}
