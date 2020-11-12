package hdfs.session6;

/**
 * @author: reiserx
 * Date:2020/7/23
 * Des:构建者模式
 */
public class BuilderPatternDemo {


    public static void main(String[] args) {
        Student student = new ConCreateStudent()
                .setField1("aaa")
                .setField2("bbb")
                .setField3("ccc")
                .build();

        System.out.println(student);

    }

    public interface Builder {
        Builder setField1(String field1);

        Builder setField2(String field2);

        Builder setField3(String fiedl3);

        Student build();
    }

    public static class ConCreateStudent implements Builder {
        Student student = new Student();

        public Builder setField1(String field1) {
            System.out.println("复杂操作1");
            student.setfield1(field1);
            return this;
        }

        public Builder setField2(String field2) {
            System.out.println("复杂操作2");
            student.setfield2(field2);
            return this;
        }

        public Builder setField3(String fiedl3) {
            System.out.println("复杂操作3");
            student.setfield3(fiedl3);
            return this;
        }

        public Student build() {
            return student;
        }
    }

    static class Student {

        private String field1;
        private String field2;
        private String field3;

        public String getfield1() {
            return field1;
        }

        public void setfield1(String field1) {
            this.field1 = field1;
        }

        public String getfield2() {
            return field2;
        }

        public void setfield2(String field2) {
            this.field2 = field2;
        }

        public String getfield3() {
            return field3;
        }

        public void setfield3(String field3) {
            this.field3 = field3;
        }

        @Override
        public String toString() {
            return "Student{" +
                    "field1='" + field1 + '\'' +
                    ", field2='" + field2 + '\'' +
                    ", field3='" + field3 + '\'' +
                    '}';
        }
    }
}
