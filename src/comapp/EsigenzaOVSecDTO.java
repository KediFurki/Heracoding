package comapp;

import java.util.Objects;
import java.util.Set;

public class EsigenzaOVSecDTO {
   private String esigenza;
   private Set<TimeoutOVSecDTO> timeouts;

   public EsigenzaOVSecDTO() {
   }

   public String getEsigenza() {
      return this.esigenza;
   }

   public void setEsigenza(String esigenza) {
      this.esigenza = esigenza;
   }

   public Set<TimeoutOVSecDTO> getTimeouts() {
      return this.timeouts;
   }

   public void setTimeouts(Set<TimeoutOVSecDTO> timeouts) {
      this.timeouts = timeouts;
   }

   public boolean equals(Object o) {
      if (this == o) {
         return true;
      } else if (o != null && this.getClass() == o.getClass()) {
         EsigenzaOVSecDTO that = (EsigenzaOVSecDTO)o;
         return Objects.equals(this.esigenza, that.esigenza);
      } else {
         return false;
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.esigenza});
   }
}