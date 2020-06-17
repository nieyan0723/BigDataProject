import generate.DataGen;
import sort.DataSort;
import validate.DataValidate;
import org.apache.hadoop.util.ProgramDriver;

public class ProjectDriver {

    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver pgd = new ProgramDriver();
        try {
            pgd.addClass("datagen", DataGen.class, "Generate data for the datasort");
            pgd.addClass("datasort", DataSort.class, "Run the datasort");
            pgd.addClass("datavalidate", DataValidate.class, "Checking results of datasort");
            exitCode = pgd.run(argv);
        } catch (Throwable e) {
            e.printStackTrace();
        }

        System.exit(exitCode);
    }
}