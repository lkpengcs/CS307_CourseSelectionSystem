package Implementation;

import cn.edu.sustech.cs307.database.SQLDataSource;
import cn.edu.sustech.cs307.dto.Department;
import cn.edu.sustech.cs307.exception.EntityNotFoundException;
import cn.edu.sustech.cs307.exception.IntegrityViolationException;
import cn.edu.sustech.cs307.service.DepartmentService;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class ReferenceDepartmentService implements DepartmentService {
    @Override
    public int addDepartment(String name) {
        Integer enterInfoId = 1;
        try {
            int exist = 0;
            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            String sql = "SELECT count(*) from department where name = ?";
            PreparedStatement pst = connection.prepareStatement(sql);
            pst.setString(1, name);
            ResultSet resultSet = pst.executeQuery();
            if (resultSet.next()) {
                exist = resultSet.getInt(1);
            }
            if (exist > 0) {
                connection.close();
                throw new IntegrityViolationException();
            }
            PreparedStatement stmt = connection.prepareStatement("insert into Department(name) values(?);", Statement.RETURN_GENERATED_KEYS);
            stmt.setString(1, name);
            //stmt.execute();
            stmt.executeUpdate();
            ResultSet rst = stmt.getGeneratedKeys();
            if (rst.next()) {
                enterInfoId = rst.getInt(1);
                //System.out.print("获取自动增加的id号=="+enterInfoId+"\n");
            }
            rst.close();
            pst.close();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return enterInfoId;
    }

    @Override
    public void removeDepartment(int departmentId) {
        try {
            int exist = 0;
            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            String sql = "SELECT count(*) from department where id = ?";
            PreparedStatement pst = connection.prepareStatement(sql);
            pst.setInt(1, departmentId);
            ResultSet resultSet = pst.executeQuery();
            if (resultSet.next()) {
                exist = resultSet.getInt(1);
            }
            if (exist > 0) {
                //删掉对应major的student
                PreparedStatement stmt = connection.prepareStatement("delete from student where id=(select student.id from department join major on departmentId=Department.id \n" +
                        "    join student on majorid=major.id where departmentId=?);");
                stmt.setInt(1, departmentId);
                stmt.execute();
                //删掉对应department的major
                stmt = connection.prepareStatement("delete from major where id=(select major.id from major join department on departmentId=department.id\n" +
                        "    where departmentId=?);");
                stmt.setInt(1, departmentId);
                stmt.execute();
            } else {
                connection.commit();
                connection.close();
                throw new EntityNotFoundException();
            }
            connection.commit();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<Department> getAllDepartments() {
        ResultSet rst = null;
        List<Department> dlist=new ArrayList<>();
        try {
            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            String sql = "select * from department ";
            PreparedStatement pst = connection.prepareStatement(sql);

            ResultSet resultSet = pst.executeQuery();

            while(resultSet.next()){
                int id=resultSet.getInt(1);
                String name=resultSet.getString(2);
                Department d=new Department();
                d.id=id;
                d.name=name;
                dlist.add(d);
            }

            int exist = 0;
            if (resultSet.next()) {
                exist = resultSet.getInt(1);
            }
//            if (exist == 0) {
//                throw new EntityNotFoundException();
//            }
            resultSet.close();
            pst.close();
            connection.commit();
            connection.close();
        }
        //Statement.RETURN_GENERATED_KEYS:获取自动增加的id号
        catch (SQLException e) {
            e.printStackTrace();
        }
        return dlist;
    }

    @Override
    public Department getDepartment(int departmentId) {
        ResultSet rst = null;
        Department result = null;
        try {
            Connection connection = SQLDataSource.getInstance().getSQLConnection();
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            String sql = "select count(*) from department where id = ? ";
            PreparedStatement pst = connection.prepareStatement(sql);
            pst.setInt(1, departmentId);
            ResultSet resultSet = pst.executeQuery();
            int exist = 0;
            if (resultSet.next()) {
                exist = resultSet.getInt(1);
            }
            if (exist == 0) {
                connection.commit();
                connection.close();
                throw new EntityNotFoundException();
            }
            PreparedStatement stmt = connection.prepareStatement("select * from department where id=?;");
            stmt.setInt(1, departmentId);
            stmt.executeUpdate();
            rst = stmt.getGeneratedKeys();
            rst = stmt.executeQuery("select * from Department where id= ?;");
            stmt.setInt(1, departmentId);
            result.id = (int) rst.getObject(1);
            result.name = (String) rst.getObject(2);
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
}
