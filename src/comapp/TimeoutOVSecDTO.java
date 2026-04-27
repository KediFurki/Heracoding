
package comapp;

import java.util.Objects;

public class TimeoutOVSecDTO {
   private String transactionName;
   private String key;
   private String value;

   public TimeoutOVSecDTO() {
   }

   public String getKey() {
      return this.key;
   }

   public void setKey(String key) {
      this.key = key;
   }

   public String getValue() {
      return this.value;
   }

   public void setValue(String value) {
      this.value = value;
   }

   public String getTransactionName() {
      return this.transactionName;
   }

   public void setTransactionName(String transactionName) {
      this.transactionName = transactionName;
   }

   public boolean equals(Object o) {
      if (this == o) {
         return true;
      } else if (o != null && this.getClass() == o.getClass()) {
         TimeoutOVSecDTO that = (TimeoutOVSecDTO)o;
         return Objects.equals(this.transactionName, that.transactionName) && Objects.equals(this.key, that.key);
      } else {
         return false;
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.transactionName, this.key});
   }
}