import org.apache.flink.connector.kafka.source.reader.ShareGroupBatchManagerTest;

public class TestRunner {
    public static void main(String[] args) {
        ShareGroupBatchManagerTest test = new ShareGroupBatchManagerTest();
        
        try {
            System.out.println("=== Running ShareGroupBatchManagerTest ===");
            
            System.out.println("1. Running testBatchAdditionAndRetrieval...");
            test.testBatchAdditionAndRetrieval();
            System.out.println("✅ testBatchAdditionAndRetrieval PASSED");
            
            System.out.println("2. Running testCheckpointLifecycle...");
            test.testCheckpointLifecycle();
            System.out.println("✅ testCheckpointLifecycle PASSED");
            
            System.out.println("3. Running testStateRestoration...");
            test.testStateRestoration();
            System.out.println("✅ testStateRestoration PASSED");
            
            System.out.println("\n🎉 All ShareGroup BatchManager tests PASSED!");
            
        } catch (Exception e) {
            System.err.println("❌ Test failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}