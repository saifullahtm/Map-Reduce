package AkshayMapReduce;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Scanner;


public class Filter {

	public static void main(String[] args) throws IOException{

		Scanner in = new Scanner(new FileReader("Task2/part-r-00000"));
        
		String Date = new String();
		int DateMaxAccidents = Integer.MIN_VALUE;
		
		String Borough  = new String();
		int BoroughMaxAccidents = Integer.MIN_VALUE;
		
		String Zip  = new String();
		int ZipMaxAccidents = Integer.MIN_VALUE;
		
		String VehicleType = new String();
		int VehicleTypeMaxAccidents = Integer.MIN_VALUE;
		
		String YearPpInured = new String();
		int YearPpInuredMaxAccidents = Integer.MIN_VALUE;
		
		String YearPpKilled = new String();
		int YearPpKilledMaxAccidents = Integer.MIN_VALUE;
		
		String cyclist = new String();
		int cyclistMaxAccidents = Integer.MIN_VALUE;
		
		String motorists = new String();
		int motoristsMaxAccidents = Integer.MIN_VALUE;
		
		while(in.hasNext()) {

			String str = in.nextLine();
			if(str.equals("")) continue;
			
			
			if(str.startsWith("Date_")) {
				String temp = str.substring(5);
				char c = (char)(9);
				int index = temp.lastIndexOf(c);
				int index1 = temp.lastIndexOf(" ");
				index = Math.max(index, index1);
				String first = temp.substring(0, index);
				int val = Integer.parseInt(temp.substring(index+1, temp.length()));
				if(val>DateMaxAccidents) {
					DateMaxAccidents = val;
					Date = first;
				}
			}
			else if(str.startsWith("Time_")) {
				
				String temp = str.substring(5);
				char c = (char)(9);
				int index = temp.lastIndexOf(c);
				int index1 = temp.lastIndexOf(" ");
				index = Math.max(index, index1);
				String first = temp.substring(0, index);
				int val = Integer.parseInt(temp.substring(index+1, temp.length()));
				if(val>BoroughMaxAccidents) {
					BoroughMaxAccidents = val;
					Borough = first;
				}
			}
			else if(str.startsWith("Zip_")) {
				
				String temp = str.substring(4);
				char c = (char)(9);
				int index = temp.lastIndexOf(c);
				int index1 = temp.lastIndexOf(" ");
				index = Math.max(index, index1);
				String first = temp.substring(0, index);
				int val = Integer.parseInt(temp.substring(index+1, temp.length()));
				if(val>ZipMaxAccidents) {
					ZipMaxAccidents = val;
					Zip = first;
				}
				
			}
			else if(str.startsWith("VechileType_")) {
				
				String temp = str.substring(12);
				char c = (char)(9);
				int index = temp.lastIndexOf(c);
				int index1 = temp.lastIndexOf(" ");
				index = Math.max(index, index1);
				String first = temp.substring(0, index);
				int val = Integer.parseInt(temp.substring(index+1, temp.length()));
				if(val>VehicleTypeMaxAccidents) {
					VehicleTypeMaxAccidents = val;
					VehicleType = first;
				}
				
			}
			else if(str.startsWith("YearPpInjured_")) {
				
				String temp = str.substring(14);
				char c = (char)(9);
				int index = temp.lastIndexOf(c);
				int index1 = temp.lastIndexOf(" ");
				index = Math.max(index, index1);
				String first = temp.substring(0, index);
				int val = Integer.parseInt(temp.substring(index+1, temp.length()));
				if(val>YearPpInuredMaxAccidents) {
					YearPpInuredMaxAccidents = val;
					YearPpInured = first;
				}
				
			}
			else if(str.startsWith("YearPpKilled_")) {
				
				String temp = str.substring(13);
				char c = (char)(9);
				int index = temp.lastIndexOf(c);
				int index1 = temp.lastIndexOf(" ");
				index = Math.max(index, index1);
				String first = temp.substring(0, index);
				int val = Integer.parseInt(temp.substring(index+1, temp.length()));
				if(val>YearPpKilledMaxAccidents) {
					YearPpKilledMaxAccidents = val;
					YearPpKilled = first;
				}
			}
			else if(str.startsWith("cyclistInjuredKilled_")) {
				String temp = str.substring(21);
				char c = (char)(9);
				int index = temp.lastIndexOf(c);
				int index1 = temp.lastIndexOf(" ");
				index = Math.max(index, index1);
				String first = temp.substring(0, index);
				int val = Integer.parseInt(temp.substring(index+1, temp.length()));
				if(val>cyclistMaxAccidents) {
					cyclistMaxAccidents = val;
					cyclist = first;
				}
				
			}
			else if(str.startsWith("motoristsInjuredKilled_")) {
				String temp = str.substring(23);
				char c = (char)(9);
				int index = temp.lastIndexOf(c);
				int index1 = temp.lastIndexOf(" ");
				index = Math.max(index, index1);
				String first = temp.substring(0, index);
				int val = Integer.parseInt(temp.substring(index+1, temp.length()));
				if(val>motoristsMaxAccidents) {
					motoristsMaxAccidents = val;
					motorists = first;
				}
			}
		}
		System.out.print("1. Date on which maximum number of accidents took place:  ");
		System.out.println(Date + "  " + DateMaxAccidents);
		System.out.print("2. Borough with maximum count of accident fatality:  ");
		System.out.println(Borough + " " + BoroughMaxAccidents);
		System.out.print("3. Zip with maximum count of accident fatality:  ");
		System.out.println(Zip + " " + ZipMaxAccidents);
		System.out.print("4. Which vehicle type is involved in maximum accidents:  ");
		System.out.println(VehicleType + " " + VehicleTypeMaxAccidents);
		System.out.print("5. Year in which maximum Number Of Persons and Pedestrians Injured:  ");
		System.out.println(YearPpInured + " " + YearPpInuredMaxAccidents);
		System.out.print("6. Year in which maximum Number Of Persons and Pedestrians Killed:  ");
		System.out.println(YearPpKilled + " " + YearPpKilledMaxAccidents);
		System.out.print("7. Year in which maximum Number Of Cyclist Injured and Killed:  ");
		System.out.println(cyclist + " " + cyclistMaxAccidents);
		System.out.print("8. Year in which maximum Number Of Motorist Injured and Killed:  ");
		System.out.println(motorists + " " + motoristsMaxAccidents);
		
		
	}

}
