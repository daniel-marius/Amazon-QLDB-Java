package lab2;

import com.amazon.ion.*;
import com.amazon.ion.system.*;
import com.amazon.ion.util.*;

import software.amazon.awssdk.services.qldbsession.*;
import software.amazon.qldb.*;
import software.amazon.qldb.exceptions.TransactionAbortedException;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;

public class App {

    public static void main(String[] args) throws Exception {
        // buildAndWriteIon();
        // readAndUpdateIon();
        // abortUpdate();
        deleteDocument();
    }
    
    private static void buildAndWriteIon() throws Exception {
        IonSystem ionSys = IonSystemBuilder.standard().build();
        
        IonStruct personDocument = ionSys.newEmptyStruct();
        
        personDocument.put("PersonId").newString("123456789");
        personDocument.put("FirstName").newString("John");
        personDocument.put("LastName").newString("Doe");
        personDocument.put("MoneyWallet").newDecimal(new BigDecimal("31.24"));
        personDocument.put("DateOfBirth").newTimestamp(Timestamp.forDay(1970, 7, 4));
        personDocument.put("NumberOfLegs").newInt(2);
        personDocument.put("LijesGreenBeans").newBool(false);
        
        IonList items = personDocument.put("ThingsInPocket").newEmptyList();
        
        items.add().newString("keys");
        items.add().newString("pocketknife");
        items.add().newString("lint");
        items.add().newString("pack of gum");
        
        // Add a subdocument
        IonStruct homeAddress = personDocument.put("HomeAddress").newEmptyStruct();
        homeAddress.put("Street1").newString("123 Main Street");
        homeAddress.put("City").newString("Beverly Hills");
        homeAddress.put("State").newString("CA");
        homeAddress.put("Zip").newString("90210");
        
        IonStruct workAddress = ionSys.newEmptyStruct();
        workAddress.put("Street1").newString("12 Elm Street");
        workAddress.put("City").newString("Los Angeles");
        workAddress.put("State").newString("CA");
        workAddress.put("Zip").newString("90001");
        personDocument.put("WorkAddress", workAddress);
        
        System.out.println(personDocument.toPrettyString());
        
        // Amazon QLDB Driver Initialization
        QldbSessionClientBuilder sessionClientBuilder = QldbSessionClient.builder();
        QldbDriver driver = QldbDriver
            .builder()
            .ledger("ion-lab")
            .sessionClientBuilder(sessionClientBuilder)
            .build();
        
        Result result = driver.execute(txn -> {
           return txn.execute("INSERT INTO Person VALUE ?", personDocument); 
        });
        
        // Get documentId
        String documentId = null;
        Iterator<IonValue> iter = result.iterator();
        while (iter.hasNext()) {
            IonValue obj = iter.next();
            if (obj instanceof IonStruct) {
                IonStruct val = (IonStruct) obj;
                IonString str = (IonString) val.get("documentId");
                documentId = str.stringValue();
                break;
            }
        }
        
        System.out.println("\n\nInserted Person Document: " + documentId + "\n\n");
        
    }
    
    private static void readAndUpdateIon() throws Exception {
        // Amazon QLDB Driver Initialization
        QldbSessionClientBuilder sessionClientBuilder = QldbSessionClient.builder();
        QldbDriver driver = QldbDriver
            .builder()
            .ledger("ion-lab")
            .sessionClientBuilder(sessionClientBuilder)
            .build();
        
        try {
            
            driver.execute(txn -> {
               Result result = txn.execute("SELECT * FROM Person WHERE PersonId = '123456789'");
               
               IonStruct personDocument = null;
               Iterator<IonValue> iter = result.iterator();
               while (iter.hasNext()) {
                   IonValue obj = iter.next();
                   if (obj instanceof IonStruct) {
                       personDocument = (IonStruct) obj;
                       break;
                   }
               }
               
               if (personDocument == null) {
                   return;
               }
               
               personDocument.put("FirstName").newString("Johnathan");
               IonStruct homeAddress = (IonStruct) personDocument.remove("HomeAddress");
               homeAddress.put("Type").newString("home");
               
               IonStruct workAddress = (IonStruct) personDocument.remove("WorkAddress");
               homeAddress.put("Type").newString("work");
               
               IonList addresses = personDocument.put("Addresses").newEmptyList();
               addresses.add(homeAddress);
               addresses.add(workAddress);
               
               txn.execute("UPDATE Person AS p SET p = ? WHERE PersonId = '123456789'", personDocument);  
           
               System.out.println(personDocument.toPrettyString());
           
           });
            
        } catch (TransactionAbortedException e) {
            System.out.println("The transaction was aborted!");
        }
    }
    
    private static void abortUpdate() throws Exception {
        // Amazon QLDB Driver Initialization
        QldbSessionClientBuilder sessionClientBuilder = QldbSessionClient.builder();
        QldbDriver driver = QldbDriver
            .builder()
            .ledger("ion-lab")
            .sessionClientBuilder(sessionClientBuilder)
            .build();
            
        try {
            
           driver.execute(txn -> {
               Result result = txn.execute("SELECT * FROM Person WHERE PersonId = '123456789'");
               
               IonStruct personDocument = null;
               Iterator<IonValue> iter = result.iterator();
               while (iter.hasNext()) {
                   IonValue obj = iter.next();
                   if (obj instanceof IonStruct) {
                       personDocument = (IonStruct) obj;
                       break;
                   }
               }
               
               if (personDocument != null) {
                   personDocument.put("YouWillNeverSeeThisField").newString("...OR WILL YOU????");
                   
                   txn.execute("UPDATE Person AS p SET p = ? WHERE PersonId = '123456789'", personDocument);
                   System.out.println("Canceling transaction...");
                   txn.abort();
                   System.out.println("Did i get here?");
               }
           
            });
            
        } catch (TransactionAbortedException e) {
            System.out.println("The transaction was aborted!");
        }
    }
        
    private static void deleteDocument() throws Exception {
        // Amazon QLDB Driver Initialization
        QldbSessionClientBuilder sessionClientBuilder = QldbSessionClient.builder();
        QldbDriver driver = QldbDriver
            .builder()
            .ledger("ion-lab")
            .sessionClientBuilder(sessionClientBuilder)
            .build();
        
        Result result = driver.execute(txn -> {
           return txn.execute("DELETE FROM Person WHERE PersonId = '123456789'"); 
        });
        
        
        System.out.println("Result: " + result);
        
    }
}
