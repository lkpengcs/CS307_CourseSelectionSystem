package Implementation;

import cn.edu.sustech.cs307.database.SQLDataSource;
import cn.edu.sustech.cs307.dto.Course;
import cn.edu.sustech.cs307.dto.Department;
import cn.edu.sustech.cs307.dto.Major;
import cn.edu.sustech.cs307.exception.EntityNotFoundException;
import cn.edu.sustech.cs307.exception.IntegrityViolationException;
import cn.edu.sustech.cs307.service.*;


import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class ReferenceMajorService implements MajorService {
    @Override
    public int addMajor(String name, int departmentId) {
        Integer enterInfoId = 1;
        try {
            //if (departmentId <= 0 || departmentId > Integer.MAX_VALUE) {
//                if(departmentId<=0){
//                throw new IntegrityViolationException();
//            }
            int exist = 0;
            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            String sql="SELECT count(*) from major where name = ? and departmentid=?;";
            PreparedStatement pst = connection.prepareStatement(sql);
            pst.setString(1,name);
            pst.setInt(2,departmentId);
            ResultSet resultSet = pst.executeQuery();
            if (resultSet.next()) {
                exist = resultSet.getInt(1);
            }
            if (exist>0) {
                connection.close();
                throw new IntegrityViolationException();
            }

//            String sql1="SELECT count(*) from department where departmentid=?;";
//            PreparedStatement pst1 = connection.prepareStatement(sql1);
//            pst1.setInt(1,departmentId);
//            ResultSet resultSet1 = pst.executeQuery();
//            if (resultSet1.next()) {
//                exist = resultSet1.getInt(1);
//            }
//            if (exist==0) {
//                throw new IntegrityViolationException();
//            }

            //connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt = connection.prepareStatement("insert into major(name,departmentId) values (?,?);", Statement.RETURN_GENERATED_KEYS);
            stmt.setString(1, name);
            stmt.setInt(2, departmentId);
            //stmt.execute();
            stmt.executeUpdate();
            ResultSet rst = stmt.getGeneratedKeys();
            if (rst.next()) {
                enterInfoId = rst.getInt(1);
                //System.out.print("获取自动增加的id号=="+enterInfoId+"\n");
            }
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return enterInfoId;


    }

    @Override
    public void removeMajor(int majorId) {
        try {int exist = 0;
            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            String sql="SELECT count(*) from major where id = ?";
            PreparedStatement pst = connection.prepareStatement(sql);
            pst.setInt(1,majorId);
            ResultSet resultSet = pst.executeQuery();
            if (resultSet.next()) {
                exist = resultSet.getInt(1);
            }
            if (exist>0) {
                PreparedStatement stmt = connection.prepareStatement("delete from major where id=?");
                stmt.setInt(1, majorId);
                stmt.execute();
                stmt = connection.prepareStatement("delete from student where majorId=?");
                stmt.setInt(1, majorId);
                stmt.execute();
                connection.commit();
                connection.close();
            }
            else{
                connection.commit();
                connection.close();
                throw new EntityNotFoundException();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<Major> getAllMajors() {
        ResultSet rst = null;
        List<Major> mlist=new ArrayList<>();
        try {Connection connection = SQLDataSource.getInstance().getSQLConnection();
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            String sql = "select * from major ";
            PreparedStatement pst = connection.prepareStatement(sql);

            ResultSet resultSet =  pst.executeQuery();
            while(resultSet.next()){
                int id=resultSet.getInt(1);
                String name=resultSet.getString(2);
                int did=resultSet.getInt(3);
                Major m=new Major();
                m.id=id;
                m.name=name;

                Department result=new Department();
                String sql1 = "select count(*) from department where id = ? ";
                PreparedStatement pst1 = connection.prepareStatement(sql1);
                pst1.setInt(1, did);
                ResultSet resultSet1 = pst1.executeQuery();
                int exist = 0;
                if (resultSet1.next()) {
                    exist = resultSet1.getInt(1);
                }
                if (exist == 0) {
                    connection.commit();
                    connection.close();
                    throw new EntityNotFoundException();
                }
                PreparedStatement stmt = connection.prepareStatement("select * from department where id=?;");
                stmt.setInt(1, did);
                stmt.executeUpdate();
                rst = stmt.getGeneratedKeys();
                rst = stmt.executeQuery("select * from Department where id= ?;");
                stmt.setInt(1, did);
                result.id = (int) rst.getObject(1);
                result.name = (String) rst.getObject(2);

                m.department=result;
                mlist.add(m);
            }
            connection.commit();
            connection.close();
        }
        //Statement.RETURN_GENERATED_KEYS:获取自动增加的id号
        catch (SQLException e) {
            e.printStackTrace();
        }
        List a = (List) rst;
        return mlist;


    }

    @Override
    public Major getMajor(int majorId) {
        ResultSet rst = null;
        Major result = null;

        try {Connection connection = SQLDataSource.getInstance().getSQLConnection();
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
             String sql = "select count(*) from major where id = ? ";
             PreparedStatement pst = connection.prepareStatement(sql);
             pst.setInt(1,majorId);
             ResultSet resultSet =  pst.executeQuery();
             int exist = 0;
            if (resultSet.next()){
            exist = resultSet.getInt(1);
        }
        if (exist==0){
            connection.commit();
            connection.close();
            throw new EntityNotFoundException();
        }

            PreparedStatement stmt = connection.prepareStatement("select * from major where id=?");
            stmt.setInt(1, majorId);
            stmt.executeUpdate();
            rst = stmt.getGeneratedKeys();
            rst = stmt.executeQuery("select * from major where id=?");
            stmt.setInt(1, majorId);
            result.id = (int) rst.getObject(1);
            result.name = (String) rst.getObject(2);
            int departmentId = (int) rst.getObject(3);

            ResultSet rst1 = null;
            Department result1 = null;
            try (//Connection connection1 = SQLDataSource.getInstance().getSQLConnection();
                 PreparedStatement stmt1 = connection.prepareStatement("select * from Department where id=?")) {
                stmt1.setInt(1, departmentId);
                stmt1.executeUpdate();
                rst1 = stmt1.getGeneratedKeys();
                rst1 = stmt1.executeQuery("select * from Department where id=?");
                stmt1.setInt(1, majorId);
                result1.id = (int) rst1.getObject(1);
                result1.name = (String) rst1.getObject(2);

            }

            //Statement.RETURN_GENERATED_KEYS:获取自动增加的id号
            catch (SQLException e) {
                e.printStackTrace();
            }
            result.department = result1;
            connection.commit();
            connection.close();
        }
        //Statement.RETURN_GENERATED_KEYS:获取自动增加的id号
        catch (SQLException e) {
            e.printStackTrace();
        }
//        List a=(List)rst;
//        Department result=(Department) ((List) rst).get(0);
//        return result;
        return result;

    }

    public boolean course_not_exist(String courseId) {
        ResultSet rst = null;
        Major result = null;
        int exist = 0;
        try {
//                Connection connection = SQLDataSource.getInstance().getSQLConnection();
//             PreparedStatement stmt = connection.prepareStatement("select * from course where id=?")) {
//            stmt.setString(1, courseId);
//            stmt.executeUpdate();
//            rst = stmt.getGeneratedKeys();
//            rst = stmt.executeQuery("select * from course where id=?");
//            stmt.setString(1, courseId);
//            connection.close();

            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            String sql = "SELECT count(*) from course where id = ?";
            PreparedStatement pst = connection.prepareStatement(sql);
            pst.setString(1, courseId);
            ResultSet resultSet = pst.executeQuery();
            if (resultSet.next()) {
                exist = resultSet.getInt(1);
            }
            connection.commit();
            connection.close();
            if (exist == 0) {
                throw new IntegrityViolationException();
            }

        }
        //Statement.RETURN_GENERATED_KEYS:获取自动增加的id号
        catch (SQLException e) {
            e.printStackTrace();
        }
//        List a=(List)rst;
//        Department result=(Department) ((List) rst).get(0);
//        return result;

        if (exist==0)
            return true;
        else
            return false;

    }

    public boolean major_not_exist(int Id) {
        ResultSet rst = null;
        Major result = null;
        int exist = 0;
        try {
//                Connection connection = SQLDataSource.getInstance().getSQLConnection();
//             PreparedStatement stmt = connection.prepareStatement("select * from course where id=?")) {
//            stmt.setString(1, courseId);
//            stmt.executeUpdate();
//            rst = stmt.getGeneratedKeys();
//            rst = stmt.executeQuery("select * from course where id=?");
//            stmt.setString(1, courseId);
//            connection.close();

            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            String sql = "SELECT count(*) from major where id = ?";
            PreparedStatement pst = connection.prepareStatement(sql);
            pst.setInt(1, Id);
            ResultSet resultSet = pst.executeQuery();
            if (resultSet.next()) {
                exist = resultSet.getInt(1);
            }
            connection.commit();
            connection.close();
            if (exist == 0) {
                throw new IntegrityViolationException();
            }

        }
        //Statement.RETURN_GENERATED_KEYS:获取自动增加的id号
        catch (SQLException e) {
            e.printStackTrace();
        }
//        List a=(List)rst;
//        Department result=(Department) ((List) rst).get(0);
//        return result;

        if (exist==0)
            return true;
        else
            return false;

    }

    @Override
    public void addMajorCompulsoryCourse(int majorId, String courseId) {
        //Major m = getMajor(majorId);
        //没有getCourse(String courseId)方法 直接在这里实现
        try {
            if (major_not_exist(majorId) || course_not_exist(courseId)) {
                throw new IntegrityViolationException();
            }

            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt = connection.prepareStatement("insert into major_course(majorId, courseId, type) VALUES (?,?,?);", Statement.RETURN_GENERATED_KEYS);
            stmt.setInt(1, majorId);
            stmt.setString(2, courseId);
            stmt.setInt(3, 1);
            //stmt.execute();
            stmt.executeUpdate();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void addMajorElectiveCourse(int majorId, String courseId) {
        //Major m = getMajor(majorId);
        //没有getCourse(String courseId)方法 直接在这里实现
        try {
            if (major_not_exist(majorId) || course_not_exist(courseId)) {
                throw new IntegrityViolationException();
            }
            int exist = 0;
            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            String sql="SELECT count(*) from major_course where majorid = ? and courseid=? ;";
            PreparedStatement pst = connection.prepareStatement(sql);
            pst.setInt(1,majorId);
            pst.setString(2,courseId);
            ResultSet resultSet = pst.executeQuery();
            if (resultSet.next()) {
                exist = resultSet.getInt(1);
            }
            if (exist>0) {
                connection.close();
                throw new IntegrityViolationException();
            }
            //connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt = connection.prepareStatement("insert into major_course(majorId, courseId, type) VALUES (?,?,?);", Statement.RETURN_GENERATED_KEYS);
            stmt.setInt(1, majorId);
            stmt.setString(2, courseId);
            stmt.setInt(3, 2);
            //stmt.execute();
            stmt.executeUpdate();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
