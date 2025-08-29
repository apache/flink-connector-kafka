import org.apache.flink.connector.kafka.source.KafkaShareGroupSourceConfigurationTest;

public class ConfigTestRunner {
    public static void main(String[] args) {
        KafkaShareGroupSourceConfigurationTest test = new KafkaShareGroupSourceConfigurationTest();
        
        try {
            System.out.println("=== Running KafkaShareGroupSourceConfigurationTest ===");
            
            System.out.println("1. Running testTraditionalKafkaSourceConfiguration...");
            test.testTraditionalKafkaSourceConfiguration();
            System.out.println("✅ testTraditionalKafkaSourceConfiguration PASSED");
            
            System.out.println("2. Running testShareGroupSourceConfiguration...");
            test.testShareGroupSourceConfiguration();
            System.out.println("✅ testShareGroupSourceConfiguration PASSED");
            
            System.out.println("3. Running testVersionCompatibility...");
            test.testVersionCompatibility();
            System.out.println("✅ testVersionCompatibility PASSED");
            
            System.out.println("4. Running testShareGroupPropertiesValidation...");
            test.testShareGroupPropertiesValidation();
            System.out.println("✅ testShareGroupPropertiesValidation PASSED");
            
            System.out.println("5. Running testBackwardCompatibility...");
            test.testBackwardCompatibility();
            System.out.println("✅ testBackwardCompatibility PASSED");
            
            System.out.println("\n🎉 All ShareGroup Configuration tests PASSED!");
            
        } catch (Exception e) {
            System.err.println("❌ Test failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}