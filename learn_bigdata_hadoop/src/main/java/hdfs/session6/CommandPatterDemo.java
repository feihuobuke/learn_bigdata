package hdfs.session6;

/**
 * @author: reiserx
 * Date:2020/7/23
 * Des:命令模式
 */
public class CommandPatterDemo {

    public static void main(String[] args) {
        Context context = new Context(new ReadCommand());
        context.execute();
    }

    static class Context {
        Command command;

        Context(Command command) {
            this.command = command;
        }

        public void execute() {
            command.execute();
        }
    }

    interface Command {
        void execute();
    }

    static class ReadCommand implements Command {

        public void execute() {
            System.out.println("Execute Read Command");
        }
    }

    class WriteCommmand implements Command {

        public void execute() {
            System.out.println("Execute Write Command ");
        }
    }
}
