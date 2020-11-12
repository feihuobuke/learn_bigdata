package hdfs.session6;

/**
 * @author: reiserx
 * Date:2020/7/23
 * Des:
 */
public class DecoratePatternDemo {
    public static void main(String[] args) {

        Person p = new Person();
        SuperPerson sp = new SuperPerson(p);
        sp.eat();

    }

    static class SuperPerson {
        Person person;

        SuperPerson(Person person) {
            this.person = person;
        }

        public void eat() {
            System.out.println("吃十头怪兽");
            person.eat();
            System.out.println("吃五个美女");
        }
    }

    static class Person {
        public void eat() {
            System.out.println("一碗大米饭");
        }
    }
}
